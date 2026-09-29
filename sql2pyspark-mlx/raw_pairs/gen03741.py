from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
        F.countDistinct(F.col("ss.units")).alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 5)
    .orderBy(F.col("unique_products").desc())
)
