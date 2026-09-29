from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .select(
        F.col("p.stock_qty"),
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("ss.revenue"),
    )
    .orderBy(F.col("stock_qty").desc(), F.col("revenue").desc())
)
