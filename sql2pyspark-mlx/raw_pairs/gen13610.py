from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy("cumulative_amount", "id")
w4 = Window.partitionBy(F.date_trunc("month", "paid_at"), "order_id")
w_part = Window.orderBy("cumulative_amount", "id")
w6 = Window.partitionBy("status").orderBy(F.col("cumulative_amount").desc(), "id")
w_part_2 = Window.orderBy("last7_sum_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("method").orderBy("paid_at", "id")
w9 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("method").orderBy("order_order_date", "id")
w11 = Window.partitionBy("order_status").orderBy("paid_at", "id")
w12 = Window.partitionBy("order_status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select(
        "*",
        F.avg("amount").over(w).alias("last7_avg_amount"),
        F.max("amount").over(w).alias("last7_max_amount"),
        F.sum("amount").over(w).alias("last7_sum_amount"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)

with_windows = (
    metrics
    .withColumn("next_amount", F.lead("amount", 2).over(w3))
    .withColumn("paid_at_period_stddev_samp_last7_avg_amount", F.stddev_samp("last7_avg_amount").over(w4))
    .withColumn("cumulative_amount_bucket", F.ntile(4).over(w_part))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("first_known_method", F.first("method", ignorenulls=True).over(w_part_2))
)

prev_events = with_windows.withColumn("prev_paid_at", F.lag("paid_at").over(w8))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w9))

with_dims = (
    sessions_payments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.amount",
        "s.paid_at",
        "s.last7_sum_amount",
        "s.next_amount",
        "s.cumulative_amount_bucket",
        "s.first_known_method",
        "s.prev_paid_at",
        "s.is_new_burst",
        "s.burst_id",
        F.col("d.order_date"),
        F.col("d.status").alias("order_status"),
    )
)

enriched = (
    with_dims
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.*",
        F.col("o.amount").alias("order_amount"),
        F.col("o.order_date").alias("order_order_date"),
        F.col("o.status"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
)

with_gaps = (
    deduped
    .withColumn("is_new_burst_2", F.when(F.lag("paid_at").over(w11).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w11) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments_2 = (
    with_gaps
    .select(
        "id",
        "amount",
        "paid_at",
        "last7_sum_amount",
        "first_known_method",
        "burst_id",
        "order_date",
        "order_status",
        "dedup_rank",
        F.sum("is_new_burst_2").over(w12).alias("burst_id_2"),
    )
)

result = (
    sessions_payments_2
    .groupBy(F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("order_status") == "COMPLETED", 1)), F.lit(0)), 2).alias("amount_per_completed"),
        F.count(F.when(F.col("order_status") == "SHIPPED", 1)).alias("shipped_rows"),
        F.percentile_approx("burst_id_2", 0.95).alias("burst_id_2_p95"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
