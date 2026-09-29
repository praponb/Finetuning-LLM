from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(173, 192) & ~F.col("event_type").isin(["add_to_cart", "purchase", "search"]))
    .select(
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.date_trunc("MONTH", "event_time").alias("event_time_period"),
    )
)
