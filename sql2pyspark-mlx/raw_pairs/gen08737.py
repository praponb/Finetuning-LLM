from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("prev_paid_at", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("order_status").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy("running_session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.year("prev_paid_at"), "method").orderBy(F.col("last4_min_is_new_burst_2").asc_nulls_first())
w9 = Window.partitionBy(F.to_date("prev_paid_at"), "order_status").orderBy("last4_count_is_new_burst_2_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("method").orderBy("is_new_burst_2", "id")

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

enriched = (
    islands
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.status").alias("order_status"), F.col("o.order_date"))
)

with_prev = enriched.withColumn("prev_paid_at_2", F.lag("paid_at").over(w3))

gaps = (
    with_prev
    .withColumn("is_new_burst_2", F.when(F.col("prev_paid_at_2").isNull() | (F.col("paid_at") > F.col("prev_paid_at_2") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands_payments = gaps.withColumn("session_no", F.sum("is_new_burst_2").over(w4))

scored = (
    islands_payments
    .withColumn("last4_min_is_new_burst_2", F.min("is_new_burst_2").over(w5))
    .withColumn("last4_count_is_new_burst_2", F.count("is_new_burst_2").over(w5))
    .withColumn("last4_max_is_new_burst_2", F.max("is_new_burst_2").over(w5))
    .withColumn("last4_sum_is_new_burst_2", F.sum("is_new_burst_2").over(w5))
    .withColumn("running_session_no", F.sum("session_no").over(w6))
)

with_windows = (
    scored
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.avg("last4_sum_is_new_burst_2").over(w_ordered).alias("running_avg_last4_sum_is_new_burst_2"),
        F.sum("last4_sum_is_new_burst_2").over(w_ordered).alias("running_sum_last4_sum_is_new_burst_2"),
        F.count("last4_sum_is_new_burst_2").over(w_ordered).alias("running_count_last4_sum_is_new_burst_2"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("session_id").between(5, 8))
    .withColumn("last4_count_is_new_burst_2_filled", F.coalesce("last4_count_is_new_burst_2", F.lit(0)))
    .withColumn("method_clean", F.coalesce("method", F.lit("UNKNOWN")))
)

cleaned_payments = (
    cleaned
    .filter(F.col("method").eqNullSafe(F.lit("promptpay")))
    .select(
        "*",
        F.least("is_new_burst_2", "session_no").alias("least_is_new_burst_2_session_no"),
        F.date_trunc("MONTH", "order_date").alias("order_date_period"),
        F.coalesce("order_status", F.lit("UNKNOWN")).alias("resolved_order_status"),
    )
)

result = (
    cleaned_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "prev_paid_at",
        "is_new_burst",
        "order_date",
        "is_new_burst_2",
        "last4_min_is_new_burst_2",
        "last4_count_is_new_burst_2",
        "last4_max_is_new_burst_2",
        "running_session_no",
        "running_avg_last4_sum_is_new_burst_2",
        "last4_count_is_new_burst_2_filled",
        "method_clean",
        "order_date_period",
        F.dense_rank().over(w8).alias("last4_min_is_new_burst_2_dense_rank"),
        F.first("least_is_new_burst_2_session_no", ignorenulls=True).over(w9).alias("first_known_least_is_new_burst_2_session_no"),
        F.lag("session_id", 2).over(w10).alias("prev_session_id"),
    )
)
