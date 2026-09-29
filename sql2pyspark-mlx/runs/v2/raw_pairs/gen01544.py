from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(~F.col("p.method").isin(["cash", "bank_transfer", "promptpay"]) & F.col("p.order_id").isNull())
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
    )
)
