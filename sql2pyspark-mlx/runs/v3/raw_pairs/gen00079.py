from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.brand"), F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("launch_year").desc())
)
