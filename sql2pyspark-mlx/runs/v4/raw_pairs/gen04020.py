from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 3.4, "high").when(F.col("s.rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.city"),
    )
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.countDistinct(F.col("i.quantity")).alias("unique_products"),
    )
)
