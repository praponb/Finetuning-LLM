from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code", "id", "key")
w2 = Window.partitionBy("service")
w3 = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .withColumn("prev_status_code", F.lag("status_code", 1).over(w))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4))
    .withColumn("status_code_bucket", F.ntile(3).over(w3))
)
