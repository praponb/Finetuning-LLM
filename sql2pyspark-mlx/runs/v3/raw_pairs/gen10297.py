from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isin(["London", "Berlin", "Singapore"]))
    .groupBy(F.col("o.channel"))
    .agg(
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
        F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc(), F.col("channel").desc())
)
