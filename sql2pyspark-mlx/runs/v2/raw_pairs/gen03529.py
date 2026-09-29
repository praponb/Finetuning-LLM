from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(~(F.col("s.store_name").like("Dow%") | (F.col("s.region") != "South")))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
