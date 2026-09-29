from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.country"))
    .agg(F.countDistinct(F.col("ss.units")).alias("unique_products"))
    .orderBy("country")
    .limit(1)
)
