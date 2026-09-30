from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["add_to_cart", "click"]))
    .select(
        "id",
        "duration_sec",
        "session_id",
        "page",
        F.col("duration_sec").cast("string").alias("duration_sec_str"),
    )
)
