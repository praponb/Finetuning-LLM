from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.to_date("event_time") < "2024-02-01") & (F.col("duration_sec") == 195) & F.col("page").isin(["/home", "/product"])))
    .select(
        "id",
        "page",
        "event_time",
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
        F.substring("event_type", 1, 3).alias("event_type_prefix"),
    )
)
