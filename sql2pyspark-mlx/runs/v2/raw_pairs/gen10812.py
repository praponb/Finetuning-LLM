from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter(F.col("c.country").isin(["US", "UK", "SG"]))
    .select(
        F.col("oi.quantity"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("c.country"),
        F.col("c.city"),
    )
)
