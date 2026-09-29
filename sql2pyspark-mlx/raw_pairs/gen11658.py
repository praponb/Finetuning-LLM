from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.partitionBy(F.to_date("event_time"), "event_type").orderBy("duration_sec", "id")
w3 = Window.partitionBy("event_type", "page")
w4 = Window.partitionBy(F.date_trunc("month", "event_time"), "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("event_time"), "event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-10-17 14:00:00").cast("timestamp"))
    .withColumn("clean_page", F.trim("page"))
    .withColumn("event_time_label", F.date_format("event_time", "yyyy-MM"))
)

result = (
    filtered
    .select(
        "id",
        "page",
        "event_time",
        F.rank().over(w).alias("duration_sec_rank"),
        F.ntile(10).over(w2).alias("duration_sec_bucket"),
        F.avg("duration_sec").over(w3).alias("event_type_avg_duration_sec"),
        F.sum("duration_sec").over(w4).alias("running_duration_sec"),
        F.sum("duration_sec").over(w5).alias("running_duration_sec_2"),
    )
)
