from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at_period", "id")
w2 = Window.partitionBy("status").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("prev_paid_at_period", "id")
w4 = Window.partitionBy("method").orderBy("prev_paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy(F.col("prev_paid_at_period").desc(), "id")
w6 = Window.partitionBy("method").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("is_new_burst", "id")
w8 = Window.partitionBy("method").orderBy("paid_at", "id")
w9 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("method").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.to_date(F.lit("2023-10-20")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("amount") >= 1680, "High").when(F.col("amount") >= 210, "Medium").otherwise("Low").alias("amount_tier"),
        F.date_trunc("year", "paid_at").alias("paid_at_period"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

prev_events = cleaned.withColumn("prev_paid_at_period", F.lag("paid_at_period").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at_period").isNull() | (F.col("paid_at_period") > F.col("prev_paid_at_period") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_gaps = (
    sessionized
    .withColumn("new_group_flag", F.when(F.lag("prev_paid_at_period").over(w3).isNull() | (F.col("prev_paid_at_period") > F.lag("prev_paid_at_period").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w4))

enriched = (
    islands
    .select(
        "*",
        F.row_number().over(w5).alias("event_seq"),
        F.first("amount", ignorenulls=True).over(w6).alias("first_known_amount"),
        F.lag("is_new_burst", 1).over(w7).alias("prev_is_new_burst"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lag("session_id", 1, 0).over(w8).alias("prev_session_id"),
        F.sum("first_known_amount").over(w9).alias("cumulative_first_known_amount"),
        F.sum("is_new_burst").over(w10).alias("running_is_new_burst"),
    )
)

with_prev = scored.withColumn("prev_paid_at_period_2", F.lag("paid_at_period").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at_period_2").isNull() | (F.col("paid_at_period") > F.col("prev_paid_at_period_2") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("is_new_burst_tier", F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["session_id", "event_seq_tier", "is_new_burst_tier"], ["session_id", "event_seq_tier"], ["session_id"]],
        "session_id",
        "event_seq_tier",
        "is_new_burst_tier",
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount"))).alias("promptpay_total_amount"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.grouping("session_id").alias("gid"),
    )
    .orderBy(F.col("event_seq_tier").desc_nulls_last())
)
