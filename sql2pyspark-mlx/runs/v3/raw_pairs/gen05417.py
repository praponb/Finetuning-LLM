from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.month(F.col("s.opened_date")).isin([1, 9, 11]) & (F.length(F.col("s.store_name")) > 12))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("p.cost"),
    )
)
