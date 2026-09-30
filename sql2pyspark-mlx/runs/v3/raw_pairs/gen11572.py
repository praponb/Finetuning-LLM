from pyspark.sql import functions as F

base = df_web_events.filter(F.hour("event_time").between(6, 13))

summary = (
    base
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
    )
)

result = (
    summary
    .orderBy("event_cohort")
    .limit(5)
)
