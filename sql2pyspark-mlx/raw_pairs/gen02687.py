from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .groupBy(
        F.when(F.col("s.rating") >= 4.1, "high").when(F.col("s.rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        F.col("s.city"),
    )
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").desc())
)
