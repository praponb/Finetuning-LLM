from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.email").isNotNull())
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("orders.discount"),
        F.col("customers.city"),
        F.col("customers.country"),
    )
)
