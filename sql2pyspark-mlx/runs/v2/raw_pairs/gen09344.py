from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.sqft") == 10963) & F.col("ss.product_id").isin([1, 5, 6, 8]))
    .select(F.col("ss.store_id"), F.col("s.opened_date"), F.col("s.sqft"))
    .orderBy(F.col("store_id").desc())
)
