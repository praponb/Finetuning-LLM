from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time_month").desc_nulls_last(), "id")

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= "2025-10-01") & F.col("page").isNotNull())
    .select("*", F.month("event_time").alias("event_time_month"))
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
