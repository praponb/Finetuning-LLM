from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/product"]) & F.col("duration_sec").isNotNull())
    .select(
        "id",
        "event_type",
        "event_time",
        F.year("event_time").alias("event_time_year"),
        F.concat("page", F.lit(" - "), "event_type").alias("page_event_type"),
    )
)
