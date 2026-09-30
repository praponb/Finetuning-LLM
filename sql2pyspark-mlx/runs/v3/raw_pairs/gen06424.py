from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_semi")
    .filter(F.col("p.launch_date").between("2023-08-01", "2025-03-01"))
    .select(F.col("p.id"), F.col("p.brand"), F.col("p.supplier_id"))
)
