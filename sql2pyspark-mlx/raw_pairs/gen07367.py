from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.store_id").isin([1, 2, 4, 5]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("s.store_name"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
    )
)
