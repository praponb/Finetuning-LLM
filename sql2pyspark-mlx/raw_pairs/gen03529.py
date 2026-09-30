from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "host")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

parsed = (
    flattened
    .select(
        "host",
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    parsed
    .filter(~F.col("key").isin(["trace", "version"]))
    .withColumn("latency_ms_filled", F.coalesce("latency_ms", F.lit(0)))
)

with_windows = base.withColumn("key_sum_flag_bit4", F.sum("flag_bit4").over(w))

matched_keys = (
    with_windows
    .select("host")
    .intersectAll(
        df_app_logs
        .filter(~F.col("level").isin(["WARN", "INFO", "DEBUG"]))
        .select("host")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.host") == F.col("k.host")), "inner")
    .select("s.*")
)

base_app_logs = (
    kept_rows
    .filter(F.col("host") == "h2")
    .select(
        "latency_ms",
        "status_code",
        "key",
        "attr_value",
        "flag_bit4",
        "status_code_high",
        "latency_ms_filled",
        "key_sum_flag_bit4",
        (F.col("flag_bit4") - F.col("latency_ms_filled")).alias("flag_bit4_minus_latency_ms_filled"),
        F.coalesce("key", F.lit("N/A")).alias("key_clean"),
    )
)

result = (
    base_app_logs
    .groupBy("key")
    .agg(
        F.percentile_approx("latency_ms_filled", 0.5).alias("latency_ms_filled_p50"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count"),
        F.size(F.collect_set("key")).alias("num_keys"),
    )
    .filter(F.col("latency_ms_filled_p50") < 1000000)
    .orderBy(F.col("latency_ms_filled_p50").desc_nulls_first())
)
