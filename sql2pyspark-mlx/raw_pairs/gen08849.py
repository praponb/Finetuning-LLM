from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.min("cost").alias("min_shipment_cost"))

gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "burst_id",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "PRIORITY").when(F.col("status").rlike("^(REFUNDED)$"), "GROUP_B").otherwise("EXTENDED").alias("status_zone"),
        F.col("min_shipment_cost"),
    )
)
