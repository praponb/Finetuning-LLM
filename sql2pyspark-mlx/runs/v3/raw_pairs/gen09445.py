from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2024-07-01") & (F.col("event_time") >= "2023-10-01"))
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cart"),
    )
    .filter(F.col("duration_sec_count") >= 2)
    .orderBy(F.col("pct_cart").desc())
)
