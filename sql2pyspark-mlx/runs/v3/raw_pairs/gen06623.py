from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.city") == "Berlin") | F.col("c.name").like("%Grace%"))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("c.name"),
        F.col("c.city"),
    )
    .orderBy(F.col("id").desc())
)
