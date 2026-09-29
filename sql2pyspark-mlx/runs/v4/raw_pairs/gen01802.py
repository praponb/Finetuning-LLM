from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_payments
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "is_new_session",
        "session_id",
        F.col("sum_shipment_cost"),
    )
    .orderBy(F.col("amount").desc_nulls_first(), "is_new_session")
)
