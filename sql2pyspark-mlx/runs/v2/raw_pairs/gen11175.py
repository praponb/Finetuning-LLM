from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(344, 450) & (F.col("event_type") != "purchase"))
    .select(
        "event_time",
        "session_id",
        F.when(F.col("event_type").isin(["add_to_cart", "search"]), "Priority").otherwise("Other").alias("event_type_group"),
    )
)
