from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([16, 25, 28]) & (F.col("duration_sec") > 471))
    .select(
        "id",
        "duration_sec",
        "session_id",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
    )
)
