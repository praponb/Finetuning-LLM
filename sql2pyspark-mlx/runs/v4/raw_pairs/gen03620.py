from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.method").isin(["bank_transfer", "cash", "promptpay"]))
    .select(
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
    )
)
