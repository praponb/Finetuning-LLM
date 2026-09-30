from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(F.col("p.supplier_id"), F.col("ss.units"), F.col("ss.revenue"))
)
