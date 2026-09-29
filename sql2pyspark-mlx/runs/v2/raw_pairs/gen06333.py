from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-05-26")
    .select(
        "id",
        "session_id",
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.when(F.col("event_type").isin(["search", "click"]), "Priority").otherwise("Other").alias("event_type_group"),
    )
    .orderBy(F.col("duration_sec_whole").desc())
)
