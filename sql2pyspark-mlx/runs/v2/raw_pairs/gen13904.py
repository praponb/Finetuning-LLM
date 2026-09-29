from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.city") != "Berlin")
    .groupBy(
        F.when(F.col("s.rating") >= 4.5, "high").when(F.col("s.rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(F.count(F.col("i.quantity")).alias("quantity_count"))
    .orderBy(F.col("country").desc(), "rating_bucket")
    .limit(20)
)
