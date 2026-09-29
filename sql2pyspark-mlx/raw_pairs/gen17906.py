from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_recent = Window.partitionBy("device", "customer_id")
w_hist = Window.orderBy("started_at", "id")
w7 = Window.partitionBy("segment", "browser")
w_grp = Window.partitionBy("country").orderBy("seq_no", "id")
w9 = Window.partitionBy("browser", "device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w10 = Window.partitionBy("country").orderBy("device_sum_event_seq", "id")
w_grp_2 = Window.partitionBy(F.date_trunc("MONTH", "started_at")).orderBy(F.col("event_seq_share").desc_nulls_last())
w12 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w13 = Window.partitionBy("device").orderBy(F.col("web_event_duration_sec").desc_nulls_first(), "id")
w14 = Window.partitionBy("seq_no_tier").orderBy(F.col("max_event_seq_2").desc())

scored = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.first("country", ignorenulls=True).over(w).alias("first_known_country"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.country"), F.col("b.name"), F.col("b.segment"))
)

metrics = (
    with_dims
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "first_known_country",
        "country",
        "name",
        "segment",
        F.row_number().over(w3).alias("seq_no"),
        F.sum("event_seq").over(w4).alias("event_seq_sum_7d"),
        F.sum("event_seq").over(w_recent).alias("device_sum_event_seq"),
        F.row_number().over(w_hist).alias("event_seq_2"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w7)), 4).alias("event_seq_share"),
    )
)

scored_sessions = (
    metrics
    .select(
        "id",
        "device",
        "started_at",
        "name",
        "segment",
        "seq_no",
        "device_sum_event_seq",
        "event_seq_2",
        F.ntile(3).over(w_grp).alias("seq_no_bucket"),
        F.max("event_seq_2").over(w9).alias("event_seq_2_max_30d"),
        F.lag("device_sum_event_seq", 1, 0).over(w10).alias("prev_device_sum_event_seq"),
        F.percent_rank().over(w_grp_2).alias("event_seq_share_percent_rank"),
    )
)

with_latest = (
    scored_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w12))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.segment",
        "s.seq_no",
        "s.device_sum_event_seq",
        "s.event_seq_2",
        "s.event_seq_2_max_30d",
        "s.prev_device_sum_event_seq",
        "s.event_seq_share_percent_rank",
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

deduped = (
    with_latest
    .withColumn("rn", F.row_number().over(w13))
    .filter(F.col("rn") == 1)
)

daily = (
    deduped
    .groupBy(
        F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"),
        "seq_no",
    )
    .agg(
        F.max("event_seq_2").alias("max_event_seq_2"),
        F.max(F.when(F.col("segment") == "SMB", F.col("event_seq_share_percent_rank"))).alias("smb_max_event_seq_share_percent_rank"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
    .withColumn("max_event_seq_2_dense_rank", F.dense_rank().over(w14))
)

result = (
    daily
    .select(
        "seq_no_tier",
        "seq_no",
        "smb_max_event_seq_share_percent_rank",
        "max_event_seq_2_dense_rank",
    )
)
