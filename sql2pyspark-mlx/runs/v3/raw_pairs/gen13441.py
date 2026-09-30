from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .select(
        F.col("c.city"),
        F.col("c.country"),
        F.col("c.segment"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
    )
)
