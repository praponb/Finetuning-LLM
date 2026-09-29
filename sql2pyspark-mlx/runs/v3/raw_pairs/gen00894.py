from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("payments.method") != "card")
    .select(
        F.col("payments.id"),
        F.col("payments.amount"),
        F.col("payments.order_id"),
        F.col("orders.channel"),
        F.col("orders.amount").alias("order_amount"),
        F.col("stores.sqft"),
        F.col("stores.city"),
    )
    .orderBy(F.col("amount").desc())
)
