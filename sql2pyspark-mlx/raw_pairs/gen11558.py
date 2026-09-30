from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "session_id",
        F.when(F.col("event_type").isin(["add_to_cart", "purchase"]), "Tier 1").otherwise("Other").alias("event_type_group"),
    )
)
