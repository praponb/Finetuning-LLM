from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("oi.product_id").isNull())
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("c.city"),
        F.col("c.segment"),
    )
    .orderBy(F.col("quantity").desc(), F.col("city").desc(), "id")
    .limit(1)
)
