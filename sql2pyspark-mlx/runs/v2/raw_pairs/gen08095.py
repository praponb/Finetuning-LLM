from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("sale_date").between("2024-08-01", "2025-07-01")).alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.category"))
    .orderBy(F.col("category").desc(), "id")
    .limit(10)
)
