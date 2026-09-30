from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(10, 18))
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
    )
    .filter(F.col("cart_count") >= 4)
    .orderBy("latest_event_time")
)
