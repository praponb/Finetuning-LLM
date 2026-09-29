from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(F.sum(F.col("ss.units")).alias("total_units"))
    .orderBy(F.col("total_units").desc(), "brand")
    .limit(10)
)
