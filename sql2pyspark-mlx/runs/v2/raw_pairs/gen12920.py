from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.sale_date") < "2025-07-14")
    .select(F.col("ss.product_id"), F.col("p.supplier_id"))
)
