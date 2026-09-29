from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 5, 25]) & F.col("session_id").isin([22, 26, 27]))
    .select(
        "id",
        "event_type",
        "event_time",
        F.coalesce("page", F.lit("unassigned")).alias("page_filled"),
    )
)
