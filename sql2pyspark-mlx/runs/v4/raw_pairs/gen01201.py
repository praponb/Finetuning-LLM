from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("status_code_high", "id")
w_ordered = Window.partitionBy("service").orderBy("status_code_high", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("retry_count").asc_nulls_first())
w5 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

parsed = (
    df_app_logs
    .filter(F.col("service").isin(["checkout", "auth", "search"]))
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context")["region"].alias("region"),
        F.col("context")["version"].alias("version"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    parsed
    .withColumn("status_code_high_bucket", F.ntile(5).over(w))
    .withColumn("last_known_version", F.last("version", ignorenulls=True).over(w_ordered))
    .withColumn("cumulative_latency_ms", F.sum("latency_ms").over(w3))
    .withColumn("retry_count_dense_rank", F.dense_rank().over(w_running))
    .withColumn("retry_count_avg_15m", F.avg("retry_count").over(w5))
)
