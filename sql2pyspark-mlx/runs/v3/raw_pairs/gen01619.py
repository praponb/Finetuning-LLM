from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.length(F.col("p.product_name")) < 13)
    .groupBy(
        F.when(F.col("p.category").isin(["Home", "Sports"]), "tier_1").otherwise("other").alias("category_group"),
    )
    .agg(F.round(F.avg(F.col("oi.unit_price")), 2).alias("avg_unit_price"))
    .orderBy("category_group", F.col("avg_unit_price").desc())
)
