from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("oi.unit_price") <= 250)
    .groupBy(F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"))
    .agg(
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
        F.countDistinct(F.col("oi.unit_price")).alias("unique_orders"),
    )
    .orderBy(F.col("avg_quantity").desc(), F.col("unique_orders").desc())
)
