from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .select(
        F.col("ss.revenue"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
    )
    .orderBy(F.col("cost").desc())
)
