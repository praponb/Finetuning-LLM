from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy(F.to_date("paid_at"), "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy(F.year("paid_at")).orderBy("is_cash", "id")
w5 = Window.partitionBy("status")

valid = (
    df_payments
    .filter(F.col("method").isNotNull() & (~F.col("method").eqNullSafe(F.lit("paypal"))))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.round((F.col("amount") * 1.15), 2).alias("amount_adjusted"),
        F.when(F.col("method") == "cash", 1).otherwise(0).alias("is_cash"),
    )
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

enriched = (
    islands
    .filter(F.col("amount_adjusted").between(58, 76))
    .withColumn("is_cash_max_30d", F.max("is_cash").over(w_part))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("status_stddev_samp_amount", F.stddev_samp("amount").over(w5))
)

prev_events = enriched.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    flagged
    .select(
        "id",
        "method",
        "paid_at",
        "is_cash",
        "session_no",
        "is_cash_max_30d",
        "event_seq",
        "status_stddev_samp_amount",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessions_payments
    .select("id", "paid_at", "session_no", "status_stddev_samp_amount", "island_id")
)
