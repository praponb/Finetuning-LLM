from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").isNotNull())
    .groupBy(F.col("s.region"))
    .agg(F.max(F.col("ss.sale_date")).alias("last_sale_date"))
    .orderBy("region")
    .limit(3)
)
