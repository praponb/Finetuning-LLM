from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.partitionBy("key")
w3 = Window.orderBy(F.col("status_code").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    exploded
    .withColumn("service_max_status_code", F.max("status_code").over(w))
    .withColumn("key_stddev_samp_status_code", F.stddev_samp("status_code").over(w2))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w3))
)
