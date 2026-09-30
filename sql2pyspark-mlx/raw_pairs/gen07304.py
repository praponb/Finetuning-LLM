from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(6, 9) & F.col("page").isNotNull())
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
        "page",
    )
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
    )
    .filter(F.col("click_duration_sec") > 52)
)
