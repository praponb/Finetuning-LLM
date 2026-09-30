from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.orderBy("event_time_day", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w_recent = Window.orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("session_id").orderBy(F.col("seq_no").desc())
w_part = Window.orderBy("duration_sec", "id")
w7 = Window.orderBy("duration_sec", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("session_id").isin([21, 29, 39, 40]))
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.col("event_time").cast("date").alias("event_time_day"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.lead("duration_sec", 1).over(w2).alias("next_duration_sec"),
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(5).over(w_recent).alias("duration_sec_bucket"),
    )
)

latest = (
    metrics
    .withColumn("rn_2", F.rank().over(w5))
    .filter(F.col("rn_2") == 1)
    .select("id", "page", "duration_sec")
)

scored = (
    latest
    .select(
        "id",
        "page",
        F.lag("duration_sec", 2).over(w_part).alias("prev_duration_sec"),
        F.rank().over(w).alias("duration_sec_rank"),
        F.avg("duration_sec").over(w7).alias("duration_sec_avg_3"),
        F.ntile(10).over(w_part).alias("duration_sec_bucket"),
        F.ntile(10).over(w_grp).alias("duration_sec_bucket_2"),
    )
)

report = (
    scored
    .select("id", "page", "duration_sec_bucket", "duration_sec_bucket_2")
    .orderBy(F.col("page").asc_nulls_last())
)

result = report.select("id", "page", "duration_sec_bucket", "duration_sec_bucket_2")
