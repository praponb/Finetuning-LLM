from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy("key")
w_part = Window.partitionBy(F.month("log_ts"), "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("level", "key").orderBy(F.col("latency_ms").desc_nulls_last())
w5 = Window.partitionBy("level").orderBy(F.col("latency_ms_sum_2h_minus_latency_ms_count_15m").asc_nulls_last())
w6 = Window.partitionBy("level")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

enriched = (
    flattened
    .filter(F.col("level") == "INFO")
    .select(
        "*",
        F.count("latency_ms").over(w).alias("latency_ms_count_15m"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.sum("latency_ms").over(w_part).alias("latency_ms_sum_2h"),
    )
)

base = (
    enriched
    .filter((F.col("log_ts") >= F.to_date(F.lit("2025-06-01"))) & F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "key",
        "value",
        "latency_ms_count_15m",
        "latency_ms_sum_2h",
        F.col("latency_ms_sum_2h").cast("string").alias("latency_ms_sum_2h_str"),
        (F.col("latency_ms_sum_2h") - F.col("latency_ms_count_15m")).alias("latency_ms_sum_2h_minus_latency_ms_count_15m"),
    )
)

scored = (
    base
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms_count_15m",
        "latency_ms_sum_2h",
        "latency_ms_sum_2h_str",
        F.percent_rank().over(w4).alias("latency_ms_percent_rank"),
        F.cume_dist().over(w5).alias("latency_ms_sum_2h_minus_latency_ms_count_15m_cume_dist"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w6)), 4).alias("latency_ms_share"),
    )
)

valid = (
    scored
    .filter(F.col("latency_ms_percent_rank") > 0.24)
    .select(
        "*",
        F.coalesce("latency_ms_percent_rank", F.lit(0)).alias("latency_ms_percent_rank_filled"),
        F.substring("level", 1, 3).alias("level_prefix"),
    )
)

result = (
    valid
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .cube("activity_day")
    .agg(
        F.round(F.sum("latency_ms_sum_2h") / F.nullif(F.count(F.when(F.col("level") == "WARN", 1)), F.lit(0)), 2).alias("latency_ms_sum_2h_per_warn"),
        F.count_if(F.col("level") == "DEBUG").alias("debug_events"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.grouping("activity_day").alias("grouping_id"),
    )
    .orderBy("debug_events")
)
