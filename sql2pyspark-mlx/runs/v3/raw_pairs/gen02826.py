from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "outer")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.region").isin(["North", "South"]))
    .select(
        F.col("payments.id"),
        F.col("payments.method"),
        F.col("payments.amount"),
        F.col("orders.amount").alias("order_amount"),
        F.col("stores.opened_date"),
        F.col("stores.region"),
    )
    .orderBy(F.col("order_amount").desc())
)
