from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("p.stock_qty"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("product_id").desc(), "id")
)
