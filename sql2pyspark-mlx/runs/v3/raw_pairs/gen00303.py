from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("p.amount"),
        F.col("o.store_id"),
        F.col("c.credit_limit"),
        F.trim(F.col("o.status")).alias("clean_status"),
    )
)
