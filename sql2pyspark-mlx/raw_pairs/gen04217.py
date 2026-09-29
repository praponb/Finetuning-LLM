from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.price") >= 420)
    .groupBy(
        F.when(F.col("s.rating") >= 3.7, "high").when(F.col("s.rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.col("ss.units")).alias("total_units"),
        F.count(F.col("ss.revenue")).alias("revenue_count"),
    )
)
