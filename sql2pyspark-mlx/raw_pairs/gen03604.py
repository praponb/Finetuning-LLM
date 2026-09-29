from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.region") == "West")
    .select(
        F.col("payments.id"),
        F.col("payments.paid_at"),
        F.col("payments.amount"),
        F.col("orders.order_date"),
        F.col("stores.store_name"),
        F.col("stores.sqft"),
    )
    .orderBy(F.col("order_date").desc(), F.col("amount").desc())
)
