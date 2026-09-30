from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("paid_at_hour").desc(), "id")
w2 = Window.orderBy(F.col("amount").desc())
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("prev_paid_at", "id")
w6 = Window.partitionBy("method").orderBy("prev_paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("paid_at", "id")
w8 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("method") == "promptpay")
    .withColumn("paid_at_hour", F.hour("paid_at"))
    .withColumn("paid_at_date", F.to_date("paid_at"))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

enriched = (
    base
    .select(
        "*",
        F.ntile(4).over(w_running).alias("paid_at_hour_bucket"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)

base_payments = (
    enriched
    .filter((F.col("paid_at") >= F.lit("2024-03-05").cast("date")) & F.col("method").isNotNull())
    .withColumn("status_method", F.concat("status", F.lit("/"), "method"))
)

with_prev = base_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w3))

flagged = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w4))

with_gaps = (
    islands
    .withColumn("session_start_flag", F.when(F.lag("prev_paid_at").over(w5).isNull() | (F.col("prev_paid_at") > F.lag("prev_paid_at").over(w5) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands_payments = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w6))

flagged_2 = (
    islands_payments
    .withColumn("is_new_burst_2", F.when(F.col("paid_at") - F.lag("paid_at").over(w7) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged_2.withColumn("island_id", F.sum("is_new_burst_2").over(w8))

pivoted = (
    sessions_payments
    .groupBy("method")
    .pivot("status", ["SUCCESS", "REFUNDED"])
    .agg(F.sum("paid_at_hour_bucket").alias("total"), F.count(F.lit(1)).alias("n"))
    .select(
        "method",
        F.col("SUCCESS_total").alias("success_total"),
        F.col("SUCCESS_n").alias("success_n"),
        F.col("REFUNDED_total").alias("refunded_total"),
        F.col("REFUNDED_n").alias("refunded_n"),
    )
)

result = (
    pivoted
    .select(
        "method",
        "success_total",
        "success_n",
        "refunded_total",
        "refunded_n",
        F.when(F.col("method").rlike("^(bank_transfer|cash)$"), "EXTENDED").when(F.col("method").rlike("^(card|paypal)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("method_zone"),
    )
    .orderBy(F.col("refunded_n").desc())
)
