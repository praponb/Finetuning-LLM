from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([2, 3, 9]) & (F.col("p.price") > 890))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("p.category"),
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
    .orderBy("units")
)
