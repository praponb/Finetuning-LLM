from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.sale_date") >= "2023-10-13")
    .select(
        F.col("p.brand"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
    )
)
