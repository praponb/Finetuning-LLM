from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.year(F.col("ss.sale_date")) >= 2024)
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("s.store_name"),
    )
    .orderBy("sale_date", "sqft", "id")
    .limit(5)
)
