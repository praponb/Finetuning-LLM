from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.supplier_name").isNull())
    .groupBy(
        F.when(F.col("s.rating") >= 4.3, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(
        F.countDistinct(F.col("p.cost")).alias("unique_suppliers"),
        F.count(F.col("p.stock_qty")).alias("stock_qty_count"),
    )
    .orderBy(F.col("unique_suppliers").desc(), F.col("rating_bucket").desc())
)
