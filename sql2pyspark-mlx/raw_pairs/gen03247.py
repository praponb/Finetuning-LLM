from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    lines
    .filter(F.col("status_code") == 440)
    .withColumn("running_sum_status_code", F.sum("status_code").over(w))
    .withColumn("running_min_status_code", F.min("status_code").over(w))
    .withColumn("running_avg_status_code", F.avg("status_code").over(w))
)
