from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("page")

result = (
    df_web_events
    .groupBy("page")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
    .orderBy(
        F.col("total_duration_sec").desc(),
        F.col("cumulative_duration_sec").desc(),
    )
)
