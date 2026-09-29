from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter((F.col("p.category") == "Home") & F.col("ss.store_id").isin([3, 6]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("s.region"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
    )
)
