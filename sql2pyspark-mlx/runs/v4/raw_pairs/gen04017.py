from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

metrics = (
    df_web_events
    .withColumn("prev_duration_sec", F.lag("duration_sec", 1).over(w))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    metrics
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"), "session_id")
    .agg(
        F.avg("event_seq").alias("avg_event_seq"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.count("*").alias("event_count"),
        F.max(F.when(F.col("page") == "/checkout", F.col("prev_duration_sec"))).alias("checkout_max_prev_duration_sec"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.count_if(F.col("page") == "/home").alias("home_events"),
    )
    .filter(F.col("checkout_max_prev_duration_sec") < 0)
    .orderBy(F.col("avg_event_seq").desc_nulls_first())
)
