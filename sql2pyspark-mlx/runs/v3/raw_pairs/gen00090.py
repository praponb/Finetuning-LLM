from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.month(F.col("orders.order_date")).isin([2, 7, 8]) | (F.col("payments.method") == "paypal"))
    .select(
        F.col("orders.shipping_fee"),
        F.col("orders.discount"),
        F.col("payments.method"),
    )
)
