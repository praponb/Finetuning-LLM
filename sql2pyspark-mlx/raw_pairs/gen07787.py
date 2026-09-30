from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .filter((F.size(F.col("p.tags")) >= 2) & (F.length(F.col("s.store_name")) >= 10))
    .select(F.col("ss.units"), F.col("p.cost"), F.col("s.opened_date"))
    .orderBy(F.col("cost").desc())
)
