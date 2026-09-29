from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("s.region"),
    )
    .orderBy(F.col("stock_qty").desc(), F.col("product_id").desc())
)
