from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 451, "high").when(F.col("duration_sec") >= 192, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("duration_sec_rank", F.dense_rank().over(w))
    .orderBy(F.col("duration_sec_rank").desc())
)
