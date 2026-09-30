from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc_nulls_first())

valid = (
    df_web_events
    .filter(F.to_date("event_time") >= "2025-02-28")
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec",
        F.upper("event_type").alias("event_type_upper"),
        F.trim("event_type").alias("clean_event_type"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

result = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
