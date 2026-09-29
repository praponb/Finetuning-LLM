from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").desc_nulls_last())
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").asc_nulls_last(), "id")
w3 = Window.partitionBy("page", "session_id").orderBy(F.col("duration_sec").asc_nulls_last())
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("page").orderBy("duration_sec", "id")
w6 = Window.orderBy("event_time", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("page").orderBy("dedup_rank", "id")
w8 = Window.partitionBy("page", "session_id")
w9 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w10 = Window.partitionBy(F.year("event_time"), "session_id").orderBy(F.col("duration_sec").desc(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

deduped = (
    df_web_events
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "session_id", "page", "event_time", "duration_sec")
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "dedup_rank",
        F.cume_dist().over(w3).alias("duration_sec_cume_dist"),
        F.ntile(5).over(w4).alias("duration_sec_bucket"),
        F.lead("duration_sec", 1).over(w5).alias("next_duration_sec"),
        F.avg("duration_sec").over(w6).alias("duration_sec_avg_5"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

metrics = (
    enriched
    .withColumn("page_max_duration_sec", F.max("duration_sec").over(w8))
    .withColumn("next_duration_sec_max_90d", F.max("next_duration_sec").over(w9))
    .withColumn("duration_sec_bucket_2", F.ntile(4).over(w10))
)

result = (
    metrics
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "dedup_rank",
        "duration_sec_cume_dist",
        "duration_sec_bucket",
        "next_duration_sec",
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("duration_sec_cume_dist").desc_nulls_first())
)
