from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("*").alias("num_payments"),
        F.countDistinct("amount").alias("unique_orders"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("P"), F.col("o.id") == F.col("P.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("P.num_payments"),
        F.col("P.unique_orders"),
    )
)
