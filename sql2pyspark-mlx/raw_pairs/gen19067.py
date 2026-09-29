from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("c.segment"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
)
