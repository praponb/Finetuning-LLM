from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("c.country"),
        F.col("c.age"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
