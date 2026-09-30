from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy(F.col("event_time").desc_nulls_first(), "id")

latest = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy(
        F.when(F.col("duration_sec") >= 477, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.avg(F.when(F.col("event_type") == "search", F.col("duration_sec"))).alias("search_avg_duration_sec"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_rows"),
    )
    .orderBy(F.col("duration_sec_tier").desc_nulls_first())
)
