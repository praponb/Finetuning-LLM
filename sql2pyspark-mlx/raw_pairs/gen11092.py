from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

enriched = (
    sessions_payments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.order_date") > "2024-11-01")
    .select("e.*", F.col("d.channel"))
)

gaps_2 = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w3).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = gaps_2.withColumn("burst_id", F.sum("is_new_session").over(w4))

metrics = (
    sessionized
    .select(
        "*",
        F.sum("session_no").over(w_running).alias("running_sum_session_no"),
        F.count("session_no").over(w_running).alias("running_count_session_no"),
        F.avg("session_no").over(w_running).alias("running_avg_session_no"),
        F.first("is_new_burst", ignorenulls=True).over(w6).alias("first_known_is_new_burst"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.discount"))
)

with_prev = joined.withColumn("prev_paid_at", F.lag("paid_at").over(w3))

gaps_3 = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments_2 = gaps_3.withColumn("burst_id_2", F.sum("session_start_flag").over(w4))

result = (
    sessions_payments_2
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "session_no",
        "channel",
        "is_new_session",
        "running_sum_session_no",
        F.col("avg_payment_amount"),
    )
)
