from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.price").between(500, 1190) & (F.col("ss.units") != 28))
    .select(
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
    )
    .orderBy("launch_date")
)
