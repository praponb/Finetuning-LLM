from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.segment") == "SMB")
    .groupBy(F.col("c.city"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
    .orderBy(F.col("cnt").desc(), "city")
    .limit(10)
)
