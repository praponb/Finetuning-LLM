from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter((F.col("c.credit_limit") > 10800) & (F.col("c.country") == "JP"))
    .select(
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("o.channel"),
        F.col("c.country"),
    )
)
