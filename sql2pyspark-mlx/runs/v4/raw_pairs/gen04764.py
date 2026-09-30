from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "purchase") & (F.col("duration_sec").between(218, 290) | (F.col("page") == "/checkout")))
    .select(
        "id",
        "page",
        "session_id",
        F.when(F.col("event_type") == "click", 1).otherwise(0).alias("is_click"),
    )
    .orderBy("page")
)
