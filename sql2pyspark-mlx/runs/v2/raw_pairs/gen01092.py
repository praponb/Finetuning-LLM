from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "add_to_cart", "click"]))
    .select(
        "id",
        "page",
        "session_id",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
    .orderBy(F.col("duration_sec_rounded").desc())
)
