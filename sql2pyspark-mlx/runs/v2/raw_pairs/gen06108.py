from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isin([15, 23, 28]) & (F.col("o.status") == "SHIPPED"))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.status"),
        F.col("p.amount"),
        F.col("o.shipping_fee"),
    )
    .orderBy("amount", "id")
    .limit(5)
)
