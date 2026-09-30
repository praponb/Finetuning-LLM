from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2023-05-01") & ~F.col("event_type").isin(["search", "page_view", "click"]))
    .select(
        "id",
        "session_id",
        "duration_sec",
        "page",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
    )
    .orderBy(F.col("session_id").desc(), "id")
    .limit(3)
)
