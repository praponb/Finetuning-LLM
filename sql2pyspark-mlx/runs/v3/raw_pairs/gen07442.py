from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.upper(F.col("c.name")).like("%IM %"))
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"))
    .agg(F.count(F.col("oi.quantity")).alias("quantity_count"))
    .orderBy("order_year")
)
