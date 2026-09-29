from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city").isin(["Tokyo", "Bangkok"]))
    .groupBy(F.col("o.status"))
    .agg(
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .orderBy(F.col("status").desc())
)
