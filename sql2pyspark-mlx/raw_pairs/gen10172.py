from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-07-15"))) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

result = (
    valid
    .select("event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("event_type")
    )
)
