from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "right")
    .select(
        F.col("orders.amount"),
        F.col("orders.discount"),
        F.col("orders.store_id"),
        F.col("customers.is_active"),
        F.col("customers.name"),
        F.col("customers.city"),
    )
    .orderBy(F.col("amount").desc())
)
