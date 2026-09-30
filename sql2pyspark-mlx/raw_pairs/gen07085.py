from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("num_devices").desc())

result = (
    df_sessions
    .groupBy("country", "device")
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
    )
    .withColumn("num_devices_percent_rank", F.percent_rank().over(w))
)
