from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("host")
w_hist = Window.orderBy("status_code_high", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.partitionBy("host").orderBy("status_code_high")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "payload_json",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

result = (
    flattened_attrs
    .withColumn("host_stddev_samp_status_code_high", F.stddev_samp("status_code_high").over(w_part))
    .withColumn("status_code_high_avg_2", F.avg("status_code_high").over(w_hist))
    .withColumn("status_code_high_cume_dist", F.cume_dist().over(w_running))
)
