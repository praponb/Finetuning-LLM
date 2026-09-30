from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("orders.amount"),
        F.col("customers.age"),
        F.col("customers.segment"),
        F.col("customers.email"),
    )
)
