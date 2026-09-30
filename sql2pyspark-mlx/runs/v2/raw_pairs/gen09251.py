from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.product_id").isin([3, 10]) | (F.col("ss.units") == 25))
    .select(
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.last_day(F.col("ss.sale_date")).alias("sale_date_month_end"),
    )
    .orderBy("launch_date")
)
