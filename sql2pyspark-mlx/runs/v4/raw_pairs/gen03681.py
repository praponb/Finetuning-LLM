from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2024-03-01") | F.col("session_id").isin([8, 10, 13, 15]))
    .groupBy(
        "page",
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"))
    .filter(F.col("checkout_count") >= 5)
    .orderBy("page", "event_cohort")
)
