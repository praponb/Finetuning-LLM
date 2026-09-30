from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 410, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/checkout", 1)), F.lit(0)), 2).alias("duration_sec_per_checkout"),
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.countDistinct("page").alias("distinct_pages"),
    )
)
