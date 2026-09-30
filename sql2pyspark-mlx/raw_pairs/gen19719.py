from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-08-23 15:00:00").cast("timestamp"))
    .withColumn("status_lower", F.lower("status"))
)

with_prev = valid.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_payments
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("session_no_tier", F.when(F.col("session_no") >= 8, "HIGH").otherwise("LOW"))
    .rollup("activity_month", "session_no_tier")
    .agg(
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_rows"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.grouping("activity_month").alias("grouping_id"),
    )
    .orderBy("session_no_tier")
)
