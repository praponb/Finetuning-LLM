from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.rating") >= 4.3, "high").when(F.col("s.rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.country"),
    )
    .agg(
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
        F.min(F.col("ss.units")).alias("min_units"),
    )
    .orderBy(F.col("rating_bucket").desc())
)
