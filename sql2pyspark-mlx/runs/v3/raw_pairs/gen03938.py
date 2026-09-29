from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 1) | F.col("event_type").isin(["page_view", "search", "add_to_cart"]))
    .select(
        "session_id",
        F.round((F.col("duration_sec") * 1.1), 2).alias("duration_sec_adjusted"),
        F.initcap(F.trim("page")).alias("page_title"),
    )
    .orderBy(F.col("duration_sec_adjusted").desc())
)
