from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("pct_checkout") > 45)
    .select("event_cohort", "pct_checkout", "duration_sec_count")
)
