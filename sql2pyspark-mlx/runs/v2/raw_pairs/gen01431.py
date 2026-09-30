from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.rating").between(1.9, 4.0))
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.rating") >= 3.4, "high").when(F.col("s.rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .orderBy("rating_bucket", "city")
    .limit(5)
)
