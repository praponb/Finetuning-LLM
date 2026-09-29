from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 392)
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .filter(F.col("duration_sec_range") >= 22)
    .select("page", "purchase_duration_sec", "duration_sec_range")
)
