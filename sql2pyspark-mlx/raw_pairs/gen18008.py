from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id")

enriched = (
    df_web_events
    .select(
        "*",
        F.avg("duration_sec").over(w).alias("duration_sec_avg_2_prior"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("MONTH", "event_time").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
        F.count("*").alias("event_count"),
    )
)
