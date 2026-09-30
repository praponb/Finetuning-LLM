from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")

scored = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.lead("duration_sec").over(w).alias("next_duration_sec"),
        F.lag("duration_sec").over(w).alias("prev_duration_sec"),
        F.row_number().over(w).alias("row_seq"),
    )
)

result = (
    scored
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 516, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("month", "event_time"))
    .cube("duration_sec_tier", "activity_month")
    .agg(
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.grouping("duration_sec_tier").alias("grouping_level"),
    )
    .filter(F.col("avg_row_seq") < 1000000)
)
