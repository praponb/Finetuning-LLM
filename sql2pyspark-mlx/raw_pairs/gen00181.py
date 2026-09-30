from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/search", "/checkout"]))
    .withColumn("running_min_duration_sec", F.min("duration_sec").over(w_part))
    .withColumn("running_avg_duration_sec", F.avg("duration_sec").over(w_part))
    .withColumn("running_sum_duration_sec", F.sum("duration_sec").over(w_part))
)

result = (
    scored
    .groupBy(F.date_trunc("quarter", "event_time").alias("quarter_start"))
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("running_min_duration_sec")).otherwise(0)).alias("add_to_cart_running_min_duration_sec"),
        F.round(F.avg("running_sum_duration_sec"), 2).alias("avg_running_sum_duration_sec"),
    )
    .orderBy(F.col("avg_running_sum_duration_sec").desc())
)
