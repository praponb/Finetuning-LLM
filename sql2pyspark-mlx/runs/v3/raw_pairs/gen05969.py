from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.units") >= 14)
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("p.supplier_id"),
    )
    .orderBy(F.col("supplier_id").desc())
)
