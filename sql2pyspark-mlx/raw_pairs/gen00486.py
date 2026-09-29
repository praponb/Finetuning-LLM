from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "log_ts"), "key").orderBy(F.col("is_flagged").desc_nulls_first())
w2 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy(F.month("log_ts"), "service").orderBy(F.col("is_flagged").desc())
w4 = Window.partitionBy("key").orderBy("is_flagged")
w5 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

unpacked = (
    flattened
    .select(
        "service",
        "log_ts",
        "key",
        "attr_value",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

base = (
    unpacked
    .filter(F.col("service") != "checkout")
    .select(
        "service",
        "log_ts",
        "key",
        "is_flagged",
        "status_code_high",
        F.coalesce("key", F.lit("unassigned")).alias("key_filled"),
        F.coalesce("key", F.lit("NONE")).alias("key_clean"),
    )
)

filtered = (
    base
    .filter((F.col("log_ts") >= F.to_timestamp(F.lit("2025-06-01 15:00:00"))) & F.col("key").isNotNull())
    .withColumn("status_code_high_tier", F.when(F.col("status_code_high") >= 2, "High").when(F.col("status_code_high") >= 1, "Medium").otherwise("Low"))
)

with_windows = (
    filtered
    .withColumn("is_flagged_cume_dist", F.cume_dist().over(w))
    .withColumn("is_flagged_avg_15m", F.avg("is_flagged").over(w2))
)

with_windows_app_logs = (
    with_windows
    .withColumn("is_flagged_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("is_flagged_rank", F.rank().over(w4))
    .withColumn("status_code_high_avg_30m", F.avg("status_code_high").over(w5))
)

result = (
    with_windows_app_logs
    .select(
        "key",
        "is_flagged",
        "key_filled",
        "is_flagged_cume_dist",
        "is_flagged_cume_dist_2",
        "is_flagged_rank",
        "status_code_high_avg_30m",
        F.when(F.col("key").rlike("^(trace|region)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("key_zone"),
    )
)
