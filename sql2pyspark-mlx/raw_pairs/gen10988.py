from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy(F.col("event_time").desc_nulls_first(), "id")

valid = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-07-01 12:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.col("duration_sec").cast("string").alias("duration_sec_num"),
    )
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "event_time", "duration_sec_num")
)
