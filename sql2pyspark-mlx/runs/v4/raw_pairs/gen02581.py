from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.status") == "CANCELLED")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("c.is_active"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
    )
)
