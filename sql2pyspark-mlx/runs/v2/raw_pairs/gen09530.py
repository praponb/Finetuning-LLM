from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("payments.id"),
        F.col("payments.amount"),
        F.col("payments.method"),
        F.col("orders.store_id"),
        F.col("orders.discount"),
        F.col("stores.region"),
    )
)
