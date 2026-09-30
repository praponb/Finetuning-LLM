from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/checkout", "/search"]) & F.col("session_id").isin([6, 28]) & (F.col("event_time") < "2024-07-01"))
    .select(
        "id",
        "duration_sec",
        "event_type",
        F.trim("page").alias("clean_page"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)
