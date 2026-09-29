from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.revenue"),
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("price").desc(), F.col("stock_qty").desc())
)
