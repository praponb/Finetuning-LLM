from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
        F.avg(F.when(F.col("event_type") == "page_view", F.col("duration_sec"))).alias("page_view_avg_duration_sec"),
    )
)
