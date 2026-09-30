from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.store_name").like("%Expr%"))
    .groupBy(F.col("p.brand"))
    .agg(F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"))
    .filter(F.col("avg_revenue") >= 55)
    .orderBy(F.col("avg_revenue").desc())
)
