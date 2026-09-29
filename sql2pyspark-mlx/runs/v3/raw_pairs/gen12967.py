from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.revenue"),
        F.col("s.region"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("revenue").desc(), F.col("stock_qty").desc())
)
