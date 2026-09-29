from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_duration_sec").desc())

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-11-01")
    .groupBy(
        F.when(F.col("page").isin(["/search", "/checkout"]), "tier_1").otherwise("other").alias("page_group"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("total_duration_sec").desc(), F.col("page_group").desc())
)
