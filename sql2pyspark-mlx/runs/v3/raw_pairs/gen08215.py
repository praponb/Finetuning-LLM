from pyspark.sql import functions as F

base = df_web_events.filter(F.col("event_type").isin(["purchase", "add_to_cart"]))

summary = (
    base
    .groupBy(
        "page",
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = (
    summary
    .filter(F.col("stddev_duration_sec") < 34)
    .select("page", "event_cohort", "stddev_duration_sec")
    .orderBy(F.col("event_cohort").desc())
)
