from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("p.category"),
        F.when(F.col("p.price") >= 1010, "high").when(F.col("p.price") >= 230, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(F.count(F.col("oi.unit_price")).alias("unit_price_count"))
    .orderBy("category")
)
