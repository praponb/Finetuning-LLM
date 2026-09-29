from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.month(F.col("p.launch_date")).isin([10, 11, 12]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("p.stock_qty"),
    )
)
