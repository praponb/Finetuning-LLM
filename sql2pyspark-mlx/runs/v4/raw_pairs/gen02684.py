from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.count_if(F.col("event_type") == "page_view").alias("page_view_events"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
