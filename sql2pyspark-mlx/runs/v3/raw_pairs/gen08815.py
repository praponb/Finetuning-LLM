from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter((F.col("stores.city") == "Chiang Mai") & F.col("orders.store_id").isNull())
    .select(
        F.col("payments.amount"),
        F.col("payments.paid_at"),
        F.col("orders.status"),
        F.col("stores.sqft"),
    )
    .orderBy("paid_at")
)
