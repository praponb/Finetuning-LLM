from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("order_id").isin([25, 38]))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.count("*").alias("num_order_items"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.price"),
        F.col("p.category"),
        F.col("t.avg_unit_price"),
        F.col("t.num_order_items"),
    )
    .orderBy("price", F.col("category").desc(), "id")
    .limit(3)
)
