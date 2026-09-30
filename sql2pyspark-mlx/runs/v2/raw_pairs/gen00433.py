from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("c.email"),
    )
)
