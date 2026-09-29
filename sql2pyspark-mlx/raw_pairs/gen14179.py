from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy(F.to_date("log_ts"), "level").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("log_ts"), "level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("key", "service").orderBy(F.col("latency_ms").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    exploded
    .filter(F.col("level") == "INFO")
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "key",
        "v",
        F.first("key", ignorenulls=True).over(w_ordered).alias("first_known_key"),
        F.avg("latency_ms").over(w2).alias("latency_ms_avg_30m"),
        F.dense_rank().over(w3).alias("latency_ms_dense_rank"),
    )
)
