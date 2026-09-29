from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "attr_key").orderBy("status_code", "id", "attr_key")
w2 = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id", "attr_key")
w_running = Window.partitionBy("service").orderBy("status_code", "id", "attr_key").rowsBetween(-2, -1)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "attr_key",
        F.ntile(3).over(w).alias("status_code_bucket"),
        F.ntile(5).over(w2).alias("status_code_bucket_2"),
        F.avg("status_code").over(w_running).alias("status_code_avg_2_prior"),
    )
)
