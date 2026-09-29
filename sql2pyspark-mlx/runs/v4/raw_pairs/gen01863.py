from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("ss.sale_date") > "2025-09-01")
    .groupBy(F.col("p.brand"))
    .agg((F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"))
    .orderBy(F.col("units_range").desc())
)
