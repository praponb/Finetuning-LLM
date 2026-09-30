from pyspark.sql import functions as F

result = (
    df_payments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.store_id").isin([15, 28]))
    .select(
        "s.id",
        "s.order_id",
        "s.method",
        "s.amount",
        "s.paid_at",
        "s.status",
        F.col("b.order_date"),
        F.col("b.amount").alias("order_amount"),
        F.col("b.shipping_fee"),
    )
)
