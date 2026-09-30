from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("burst_id", "id")
w5 = Window.partitionBy("method").orderBy("paid_at", "id")
w6 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("method").orderBy("seq_no", "id")
w8 = Window.partitionBy("method", "status").orderBy("is_new_session", "id")
w9 = Window.partitionBy("method")
w10 = Window.partitionBy("status").orderBy("is_new_session_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("order_id").orderBy("is_new_session_2", "id").rowsBetween(-3, -1)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

with_gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

metrics = (
    islands
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

flagged = (
    metrics
    .withColumn("is_new_session_2", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands_payments = flagged.withColumn("session_no", F.sum("is_new_session_2").over(w6))

with_windows = (
    islands_payments
    .withColumn("next_burst_id", F.lead("burst_id", 1).over(w7))
    .withColumn("next_is_new_session", F.lead("is_new_session", 2).over(w8))
    .withColumn("method_count_first_known_amount", F.count("first_known_amount").over(w9))
    .withColumn("running_seq_no", F.sum("seq_no").over(w10))
    .withColumn("session_no_avg_3_prior", F.avg("session_no").over(w11))
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.shipping_fee"), F.col("o.channel"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.status").alias("order_status"))
)

result = (
    enriched
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "is_new_session",
        "burst_id",
        "seq_no",
        "next_is_new_session",
        "running_seq_no",
        F.when(F.col("status").rlike("^(REFUNDED|SUCCESS)$"), "EXTENDED").when(F.col("status").rlike("^(FAILED)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
        F.col("avg_payment_amount"),
    )
    .orderBy(F.col("burst_id").asc_nulls_last())
)
