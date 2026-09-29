from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc())

latest = (
    df_web_events
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "session_id", "event_type", "page", "event_time")
)

result = (
    latest
    .rollup(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.max("event_time").alias("ended_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_rows"),
    )
    .orderBy("distinct_ids")
)
