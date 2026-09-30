from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("paid_at")).orderBy("amount_filled", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("amount_filled", "id")
w4 = Window.partitionBy("order_status").orderBy(F.col("shipping_fee").desc_nulls_last())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

valid = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("method").isin(["bank_transfer", "card"]))
    .withColumn("status_length", F.length("status"))
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("resolved_method", F.coalesce("method", F.lit("OTHER")))
)

enriched = (
    valid
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        "status_length",
        "amount_filled",
        F.lag("status_length", 1).over(w).alias("prev_status_length"),
        F.sum("amount_filled").over(w2).alias("running_amount_filled"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

enriched_payments = (
    enriched
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        "s.status",
        "s.running_amount_filled",
        F.col("p.amount").alias("order_amount"),
        F.col("p.shipping_fee"),
        F.col("p.status").alias("order_status"),
    )
)

latest = (
    enriched_payments
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "amount",
        "running_amount_filled",
        "order_amount",
        "dedup_rank",
        F.col("sum_shipment_cost"),
    )
    .orderBy(F.col("running_amount_filled").desc_nulls_last(), F.col("order_id").desc())
)
