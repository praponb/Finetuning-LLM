from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key", "service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("key").orderBy("latency_ms", "id", "key")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

valid = (
    flattened
    .filter((F.col("log_ts") >= F.lit("2025-06-01 14:00:00").cast("timestamp")) & F.col("key").isNotNull())
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "key",
        "attr_value",
        F.when(F.col("service") == "billing", 1).otherwise(0).alias("is_billing"),
        F.trim("host").alias("clean_host"),
    )
)

scored = (
    valid
    .filter(F.col("log_ts") < "2024-01-01")
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "key",
        "attr_value",
        F.sum("is_billing").over(w).alias("running_sum_is_billing"),
        F.min("is_billing").over(w).alias("running_min_is_billing"),
        F.avg("is_billing").over(w).alias("running_avg_is_billing"),
        F.sum("is_billing").over(w2).alias("is_billing_sum_1h"),
    )
)

scored_app_logs = (
    scored
    .withColumn("prev_running_sum_is_billing", F.lag("running_sum_is_billing").over(w3))
    .withColumn("prev_latency_ms", F.lag("latency_ms").over(w3))
    .withColumn("seq_no", F.row_number().over(w3))
)

remaining_keys = (
    scored_app_logs
    .select("id")
    .intersect(
        df_app_logs
        .filter(F.col("latency_ms") == 1056)
        .select("id")
    )
)

kept_rows = (
    scored_app_logs
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("key")
    .pivot("service", ["checkout", "auth", "search"])
    .agg(
        F.avg("running_sum_is_billing").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
