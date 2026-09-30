from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

filtered = (
    flattened
    .filter(F.col("k").isNotNull())
    .withColumn("host_group", F.when(F.col("host").isin(["h1", "h3"]), "Core").otherwise("Other"))
    .withColumn("k_clean", F.coalesce("k", F.lit("NONE")))
)

cleaned = (
    filtered
    .filter(F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 13:00:00")))
    .select(
        "*",
        F.lower("host").alias("host_lower"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

with_windows = cleaned.withColumn("status_code_count_30m", F.count("status_code").over(w))

result = (
    with_windows
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 432, "PREMIUM").otherwise("STANDARD"))
    .withColumn("hour_window", F.date_trunc("HOUR", "log_ts"))
    .rollup("status_code_tier", "hour_window")
    .agg(
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("level") == "INFO", 1)), F.lit(0)), 2).alias("status_code_per_info"),
        F.max("log_ts").alias("ended_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("log_ts").alias("last_log_ts"),
        F.round(F.sum("status_code_count_30m") / F.nullif(F.count(F.when(F.col("host") == "h2", 1)), F.lit(0)), 2).alias("status_code_count_30m_per_h2"),
        F.grouping_id().alias("grouping_level"),
    )
)
