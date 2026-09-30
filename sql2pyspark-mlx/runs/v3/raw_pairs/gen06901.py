from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2024-05-01") & ~F.col("page").isin(["/home", "/product"]))
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)
