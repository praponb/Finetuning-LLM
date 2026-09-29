from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "add_to_cart") & (F.col("duration_sec") <= 129))
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.when(F.col("duration_sec") >= 526, "High").when(F.col("duration_sec") >= 124, "Medium").otherwise("Low").alias("duration_sec_tier"),
    )
)
