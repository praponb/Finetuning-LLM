from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(F.countDistinct("amount").alias("unique_methods"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .filter(F.col("pbo.unique_methods") > 8)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("pbo.unique_methods"),
    )
    .orderBy("channel", "id")
    .limit(10)
)
