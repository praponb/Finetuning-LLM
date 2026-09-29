from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter(F.col("s.supplier_name").like("%Impor%"))
    .groupBy(F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"))
    .agg(F.count("*").alias("store_sale_count"))
    .filter(F.col("store_sale_count") > 5)
    .orderBy("launch_month")
)
