from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("p.brand"))
    .agg(F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"))
    .orderBy("avg_units")
)
