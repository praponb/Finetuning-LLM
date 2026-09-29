from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(F.col("p.supplier_id"), F.col("p.brand"))
    .agg(F.countDistinct(F.col("ss.revenue")).alias("unique_products"))
    .filter(F.col("unique_products") > 2)
)
