from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.stock_qty") != 244)
    .groupBy(
        F.when(F.col("s.rating") >= 3.8, "high").when(F.col("s.rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg(F.col("p.price")).alias("avg_price"),
        F.count(F.col("p.cost")).alias("cost_count"),
    )
    .orderBy(F.col("cost_count").desc(), "rating_bucket")
    .limit(50)
)
