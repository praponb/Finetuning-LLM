from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "prev_paid_at",
        "new_group_flag",
        F.when(F.col("method").rlike("^(paypal|card)$"), "CORE").otherwise("OTHER_ZONE").alias("method_zone"),
        F.col("sum_shipment_cost"),
    )
    .orderBy(F.col("method_zone").desc(), "sum_shipment_cost")
)
