package com.github.hippalus;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.TimeUtils;

public class CLI extends ExecutionConfig.GlobalJobParameters {

  public static final String INPUT_KEY = "input";
  public static final String OUTPUT_KEY = "output";
  public static final String DISCOVERY_INTERVAL = "discovery-interval";
  public static final String EXECUTION_MODE = "execution-mode";

  private final Path[] inputs;
  private final Path output;
  private final Duration discoveryInterval;
  private final RuntimeExecutionMode executionMode;
  private final MultipleParameterTool params;

  private CLI(
      final Path[] inputs,
      final Path output,
      final Duration discoveryInterval,
      final RuntimeExecutionMode executionMode,
      final MultipleParameterTool params) {
    this.inputs = inputs;
    this.output = output;
    this.discoveryInterval = discoveryInterval;
    this.executionMode = executionMode;
    this.params = params;
  }

  public static CLI fromArgs(final String[] args) {
    final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
    Path[] inputs = null;
    if (params.has(INPUT_KEY)) {
      inputs = params.getMultiParameterRequired(INPUT_KEY).stream()
          .map(Path::new)
          .toArray(Path[]::new);
    } else {
      System.out.println("Executing example with default input data.");
      System.out.println("Use --input to specify file input.");
    }

    Path output = null;
    if (params.has(OUTPUT_KEY)) {
      output = new Path(params.get(OUTPUT_KEY));
    } else {
      System.out.println("Printing result to stdout. Use --output to specify output path.");
    }

    Duration watchInterval = null;
    if (params.has(DISCOVERY_INTERVAL)) {
      watchInterval = TimeUtils.parseDuration(params.get(DISCOVERY_INTERVAL));
    }

    RuntimeExecutionMode executionMode = ExecutionOptions.RUNTIME_MODE.defaultValue();
    if (params.has(EXECUTION_MODE)) {
      executionMode = RuntimeExecutionMode.valueOf(params.get(EXECUTION_MODE).toUpperCase());
    }

    return new CLI(inputs, output, watchInterval, executionMode, params);
  }

  public Optional<Path[]> getInputs() {
    return Optional.ofNullable(this.inputs);
  }

  public Optional<Duration> getDiscoveryInterval() {
    return Optional.ofNullable(this.discoveryInterval);
  }

  public Optional<Path> getOutput() {
    return Optional.ofNullable(this.output);
  }

  public RuntimeExecutionMode getExecutionMode() {
    return this.executionMode;
  }

  public OptionalInt getInt(final String key) {
    if (this.params.has(key)) {
      return OptionalInt.of(this.params.getInt(key));
    }
    return OptionalInt.empty();
  }

  @Override
  public Map<String, String> toMap() {
    return this.params.toMap();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    final CLI cli = (CLI) o;
    return Arrays.equals(this.inputs, cli.inputs)
        && Objects.equals(this.output, cli.output)
        && Objects.equals(this.discoveryInterval, cli.discoveryInterval);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(this.output, this.discoveryInterval);
    result = 31 * result + Arrays.hashCode(this.inputs);
    return result;
  }

}
