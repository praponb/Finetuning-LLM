from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.year(F.col("p.launch_date")) >= 2024)
    .groupBy(
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
        F.col("o.channel"),
    )
    .agg(F.avg(F.col("oi.unit_price")).alias("avg_unit_price"))
    .orderBy(F.col("avg_unit_price").desc(), "order_month", "channel")
    .limit(5)
)
