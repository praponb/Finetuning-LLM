from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("page_group")

result = (
    df_web_events
    .groupBy(
        F.when(F.col("page").isin(["/home", "/search"]), "priority").otherwise("other").alias("page_group"),
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("cumulative_duration_sec", F.sum("total_duration_sec").over(w))
    .orderBy("total_duration_sec", "page_group")
    .limit(10)
)
