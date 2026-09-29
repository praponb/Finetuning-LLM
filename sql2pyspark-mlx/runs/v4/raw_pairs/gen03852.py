from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.orderBy("started_at", "id")
w3 = Window.partitionBy("browser")
w4 = Window.partitionBy("country", "device")
w5 = Window.orderBy("event_seq", "id")
w6 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("customer_id", "country").orderBy("row_seq_bucket", "id")
w9 = Window.orderBy("event_seq_2")
w10 = Window.partitionBy("device")
w11 = Window.partitionBy("device", "country").orderBy("event_seq_share", "id").rowsBetween(-5, Window.currentRow)
w12 = Window.orderBy("row_seq", "id")

with_windows = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w2))
)

scored = (
    with_windows
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w3)), 4))
    .withColumn("event_seq_share_2", F.round((F.col("event_seq") / F.sum("event_seq").over(w4)), 4))
    .withColumn("event_seq_2", F.row_number().over(w5))
    .withColumn("cumulative_row_seq", F.sum("row_seq").over(w6))
    .withColumn("row_seq_bucket", F.ntile(5).over(w7))
)

with_windows_sessions = (
    scored
    .select(
        "*",
        F.lag("event_seq_share_2", 1).over(w8).alias("prev_event_seq_share_2"),
        F.percent_rank().over(w9).alias("event_seq_2_percent_rank"),
        F.max("row_seq").over(w10).alias("device_max_row_seq"),
        F.avg("event_seq").over(w11).alias("event_seq_avg_5"),
        F.row_number().over(w12).alias("seq_no"),
    )
)

cleaned = (
    with_windows_sessions
    .filter((F.col("started_at") > F.lit("2023-11-23").cast("date")) & F.col("customer_id").isNotNull() & (F.col("event_seq") != 7))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "row_seq",
        "event_seq",
        "event_seq_share",
        "event_seq_share_2",
        "cumulative_row_seq",
        "row_seq_bucket",
        "prev_event_seq_share_2",
        "device_max_row_seq",
        "event_seq_avg_5",
        "seq_no",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.ceil("event_seq_share_2").alias("event_seq_share_2_whole"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"), "seq_no")
    .agg(
        F.count("*").alias("cnt"),
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
    )
    .orderBy(F.col("num_browsers").desc())
)
