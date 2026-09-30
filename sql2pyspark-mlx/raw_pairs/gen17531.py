from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc_nulls_last(), "id")

latest = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .groupBy(
        F.when(F.col("duration_sec") >= 389, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec_2"),
    )
)
