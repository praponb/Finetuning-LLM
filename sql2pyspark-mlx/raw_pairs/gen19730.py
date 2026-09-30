from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 538, "high").when(F.col("duration_sec") >= 239, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("rnk", F.dense_rank().over(w))
)
