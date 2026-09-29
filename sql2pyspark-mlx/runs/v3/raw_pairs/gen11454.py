from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([4, 8]) & F.col("ss.product_id").isin([1, 4]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
)
