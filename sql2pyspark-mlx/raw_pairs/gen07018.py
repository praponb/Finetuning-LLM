from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "host")
w2 = Window.partitionBy("level")
w3 = Window.orderBy("status_code", "id", "key")
w4 = Window.partitionBy("key", "host").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .select(
        "id",
        "status_code",
        "key",
        F.stddev_samp("status_code").over(w).alias("level_stddev_samp_status_code"),
        F.max("status_code").over(w2).alias("level_max_status_code"),
        F.ntile(3).over(w3).alias("status_code_bucket"),
        F.avg("status_code").over(w2).alias("level_avg_status_code"),
        F.sum("status_code").over(w4).alias("cumulative_status_code"),
    )
)
