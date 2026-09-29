from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2023-07-01") & (F.col("page") == "/search"))
    .select(
        "duration_sec",
        "event_time",
        "page",
        F.concat("event_type", F.lit(" "), "page").alias("event_type_page"),
    )
    .orderBy(F.col("duration_sec").desc())
)
