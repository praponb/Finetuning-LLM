from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "left")
    .select(
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
    )
    .orderBy(F.col("stock_qty").desc())
)
