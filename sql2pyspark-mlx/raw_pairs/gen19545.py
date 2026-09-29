from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.order_id").isin([7, 20, 34]))
    .groupBy(
        F.when(F.col("p.brand").isin(["Acme", "Globex"]), "tier_1").otherwise("other").alias("brand_group"),
    )
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
    .orderBy("brand_group")
    .limit(5)
)
