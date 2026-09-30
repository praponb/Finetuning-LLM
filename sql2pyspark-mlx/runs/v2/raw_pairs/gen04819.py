from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active"))
    .select(
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("c.email"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("amount").desc())
)
