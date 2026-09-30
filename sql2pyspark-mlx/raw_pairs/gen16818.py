from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code")
w2 = Window.partitionBy("host").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy(F.month("log_ts"), "attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "attr_key").orderBy(F.col("cumulative_status_code").desc_nulls_last(), "id", "attr_key")
w5 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key")
w6 = Window.partitionBy("service").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

scored = (
    lines
    .filter(F.col("attr_key").isin(["trace", "region"]))
    .withColumn("status_code_rank", F.rank().over(w))
    .withColumn("cumulative_latency_ms", F.sum("latency_ms").over(w2))
    .withColumn("cumulative_status_code", F.sum("status_code").over(w_hist))
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    latest
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w5) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w6))

base = (
    sessionized
    .filter(F.col("log_ts") > F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "attr_key",
        "status_code_rank",
        "cumulative_latency_ms",
        "cumulative_status_code",
        "island_id",
        F.upper("level").alias("level_upper"),
        F.least("rank_in_group", "cumulative_status_code").alias("least_rank_in_group_cumulative_status_code"),
        F.coalesce("attr_key", F.lit("NONE")).alias("attr_key_filled"),
    )
)

result = (
    base
    .cube(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.sum(F.when(F.col("attr_key") == "region", F.col("latency_ms"))).alias("region_total_latency_ms"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
    )
    .orderBy(F.col("avg_status_code").desc())
)
