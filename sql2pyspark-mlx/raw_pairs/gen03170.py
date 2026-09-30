from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_day").orderBy(F.col("debug_events").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("day", "log_ts").alias("activity_day"), "level")
    .agg(
        F.count_if(F.col("level") == "DEBUG").alias("debug_events"),
        F.min("log_ts").alias("first_log_ts"),
        F.count("*").alias("row_count"),
        F.min("log_ts").alias("started_at"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
    )
    .withColumn("debug_events_percent_rank", F.percent_rank().over(w))
)
