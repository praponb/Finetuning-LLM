from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec_bucket")

result = (
    df_web_events
    .filter(F.col("page") == "/home")
    .groupBy(
        F.when(F.col("duration_sec") >= 364, "high").when(F.col("duration_sec") >= 67, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
)
