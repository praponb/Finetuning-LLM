from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("duration_sec").between(155, 265))
    .groupBy("page")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("duration_sec_rank", F.rank().over(w))
    .orderBy("total_duration_sec")
)
