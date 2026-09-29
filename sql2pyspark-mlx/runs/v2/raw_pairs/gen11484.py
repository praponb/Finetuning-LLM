from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("c.credit_limit"),
        F.col("c.name"),
        F.col("c.segment"),
    )
)
