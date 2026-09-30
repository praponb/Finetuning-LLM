from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.discount") >= 0.15)
    .groupBy(F.col("o.channel"))
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
    )
    .filter(F.col("row_count") > 1)
    .orderBy("channel")
)
