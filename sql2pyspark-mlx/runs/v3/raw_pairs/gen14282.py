from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2024-04-01") & (F.col("duration_sec") != 276) & (F.col("event_type") == "add_to_cart"))
    .select(
        "id",
        "event_time",
        "session_id",
        F.when(F.col("event_type").isin(["purchase", "add_to_cart"]), "Priority").otherwise("Other").alias("event_type_group"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)
