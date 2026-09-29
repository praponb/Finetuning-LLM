from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.order_id").isin([5, 14, 30, 38]) & F.col("o.store_id").isNull())
    .select(F.col("o.status"), F.col("o.shipping_fee"), F.col("p.amount"))
    .orderBy("amount", "status")
)
