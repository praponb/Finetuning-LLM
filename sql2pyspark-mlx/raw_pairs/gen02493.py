from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("flag_bit8")
w2 = Window.partitionBy("level").orderBy(F.col("status_code_high").desc())
w3 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_first())
w4 = Window.partitionBy("flag_bit8_tier").orderBy(F.col("latency_ms_bit_and").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

parsed = (
    lines
    .filter(F.col("level") == "ERROR")
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

with_windows = (
    parsed
    .filter(~F.col("host").isin(["h4", "h3", "h2"]))
    .select(
        "*",
        F.percent_rank().over(w).alias("flag_bit8_percent_rank"),
        F.dense_rank().over(w2).alias("status_code_high_dense_rank"),
        F.dense_rank().over(w3).alias("latency_ms_dense_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("flag_bit8") > 1)
    .select(
        "*",
        (F.col("status_code_high_dense_rank") * F.col("flag_bit8_percent_rank")).alias("status_code_high_dense_rank_times_flag_bit8_percent_rank"),
        F.round("latency_ms_dense_rank", 2).alias("latency_ms_dense_rank_rounded"),
    )
)

cleaned = (
    filtered
    .filter(F.col("status_code_high_dense_rank").isNotNull())
    .select(
        "*",
        F.substring("host", 1, 2).alias("host_prefix"),
        F.coalesce("flag_bit8", F.lit(0)).alias("flag_bit8_filled"),
    )
)

summary = (
    cleaned
    .groupBy(
        F.when(F.col("flag_bit8") >= 1, "HIGH").otherwise("LOW").alias("flag_bit8_tier"),
        "host",
    )
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.count_if(F.col("host") == "h4").alias("h4_events"),
        F.sum(F.when(F.col("level") == "INFO", F.col("flag_bit8_percent_rank")).otherwise(0)).alias("info_flag_bit8_percent_rank"),
    )
    .filter(F.col("h4_events") < 1000)
    .withColumn("latency_ms_bit_and_percent_rank", F.percent_rank().over(w4))
)

result = (
    summary
    .select(
        "flag_bit8_tier",
        "host",
        "h4_events",
        "info_flag_bit8_percent_rank",
        "latency_ms_bit_and_percent_rank",
    )
    .orderBy("info_flag_bit8_percent_rank")
)
