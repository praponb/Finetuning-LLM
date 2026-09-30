from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(F.max(F.col("ss.revenue")).alias("max_revenue"))
    .orderBy(F.col("max_revenue").desc(), F.col("brand").desc())
)
