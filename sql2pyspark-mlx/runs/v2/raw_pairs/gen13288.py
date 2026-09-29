from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(
        F.col("p.amount"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
    .orderBy(F.col("store_id").desc())
)
