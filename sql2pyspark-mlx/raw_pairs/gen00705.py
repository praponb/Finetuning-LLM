from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "log_ts"), "service").orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("service")
w4 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .withColumn("latency_ms_rank", F.rank().over(w))
    .withColumn("status_code_avg_2h", F.avg("status_code").over(w2))
    .withColumn("corr_latency_ms_status_code", F.corr("latency_ms", "status_code").over(w3))
    .withColumn("latency_ms_avg_1h", F.avg("latency_ms").over(w4))
)
