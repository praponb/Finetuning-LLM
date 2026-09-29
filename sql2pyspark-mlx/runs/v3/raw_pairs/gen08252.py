from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("duration_sec") >= 246) & (F.col("session_id") == 39)) | (F.col("event_type") != "page_view"))
    .groupBy(F.date_format("event_time", "yyyy-MM").alias("event_month"), "page")
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)
