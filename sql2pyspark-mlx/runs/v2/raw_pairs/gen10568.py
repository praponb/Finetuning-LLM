from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec_bucket")

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 405, "high").when(F.col("duration_sec") >= 73, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
)
