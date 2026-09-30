from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .filter(F.col("p.supplier_id").isNull())
    .select(F.col("p.id"), F.col("p.supplier_id"), F.col("p.launch_date"))
)
