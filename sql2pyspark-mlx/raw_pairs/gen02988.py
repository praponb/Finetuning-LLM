from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "host").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "key")
w2 = Window.partitionBy("key").orderBy("log_ts", "id", "key")
w3 = Window.partitionBy("key").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

deduped = (
    lines
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "log_ts", "key")
)

filtered = (
    deduped
    .filter(F.col("key").isNotNull())
    .withColumn("log_ts_month", F.month("log_ts"))
)

gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w2).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w2) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select("id", "log_ts", "key", F.sum("is_new_session").over(w3).alias("session_no"))
)

result = (
    islands
    .select("id", "key")
    .orderBy(F.col("id").asc_nulls_last())
)
