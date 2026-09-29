from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .filter((F.col("orders.customer_id") == 1) | F.col("customers.age").between(56, 57))
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("orders.status"),
        F.col("customers.credit_limit"),
        F.col("customers.country"),
        F.col("customers.age"),
    )
    .orderBy("status")
)
