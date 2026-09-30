from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "browser").orderBy(F.col("started_at_day").desc_nulls_last(), "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("burst_id")
w_recent = Window.orderBy("started_at", "id")

valid = (
    df_sessions
    .filter(F.col("customer_id").isin([12, 21]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.col("started_at").cast("date").alias("started_at_day"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_filled"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

prev_events = latest.withColumn("prev_started_at", F.lag("started_at").over(w2))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "browser_filled",
        F.sum("is_new_session").over(w3).alias("burst_id"),
    )
)

with_windows = (
    sessionized
    .select(
        "id",
        "browser_filled",
        F.rank().over(w4).alias("burst_id_rank"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("event_seq") >= 8, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
    .orderBy(F.col("distinct_ids_2").desc_nulls_first())
)
