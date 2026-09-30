from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_status")
w3 = Window.partitionBy(F.month("paid_at")).orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("paid_at"), "method").orderBy("paid_at", "id")
w_hist = Window.partitionBy("method").orderBy(F.col("running_amount").desc(), "id")
w_recent = Window.orderBy("running_amount_scaled", "id")
w_recent_2 = Window.partitionBy("method").orderBy(F.col("running_amount").desc_nulls_first())
w8 = Window.partitionBy("method").orderBy("prev_amount", "id")
w9 = Window.partitionBy("method").orderBy("prev_amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("method").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id").eqNullSafe(F.col("d.id")), "left")
    .filter(F.col("d.amount") > 820)
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.amount",
        "e.paid_at",
        F.col("d.status").alias("order_status"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "method",
        F.sum("amount").over(w).alias("cumulative_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.lag("amount", 2).over(w4).alias("prev_amount"),
    )
)

filtered = (
    scored
    .filter(F.col("amount_share") < 0.27)
    .withColumn("running_amount_scaled", F.round((F.col("running_amount") * 1.1), 2))
)

scored_payments = (
    filtered
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("event_seq_2", F.row_number().over(w_recent))
    .withColumn("running_amount_cume_dist", F.cume_dist().over(w_recent_2))
    .withColumn("prev_amount_bucket", F.ntile(10).over(w8))
)

cleaned = (
    scored_payments
    .filter(F.col("cumulative_amount") == 7051)
    .select(
        "id",
        "method",
        "cumulative_amount",
        "amount_share",
        "prev_amount",
        "event_seq",
        "prev_amount_bucket",
        F.upper("method").alias("method_upper"),
        F.greatest("running_amount_scaled", "cumulative_amount").alias("greatest_running_amount_scaled_cumulative_amount"),
    )
)

with_windows = (
    cleaned
    .withColumn("last_known_prev_amount", F.last("prev_amount", ignorenulls=True).over(w9))
    .withColumn("first_known_cumulative_amount", F.first("cumulative_amount", ignorenulls=True).over(w10))
)

filtered_payments = (
    with_windows
    .filter(F.col("method").isNotNull() & F.col("greatest_running_amount_scaled_cumulative_amount").between(31, 36))
    .select(
        "*",
        F.round((F.col("cumulative_amount") / F.col("prev_amount")), 2).alias("cumulative_amount_per_prev_amount"),
        (F.col("cumulative_amount") - F.col("prev_amount")).alias("cumulative_amount_minus_prev_amount"),
    )
)

result = (
    filtered_payments
    .select(
        "id",
        "method",
        "cumulative_amount",
        "amount_share",
        "prev_amount",
        "prev_amount_bucket",
        "method_upper",
        "greatest_running_amount_scaled_cumulative_amount",
        "first_known_cumulative_amount",
    )
    .orderBy(F.col("amount_share").desc())
)
