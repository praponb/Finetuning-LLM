from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.stock_qty") <= 291) & (F.col("ss.units") < 14))
    .select(
        F.col("p.category"),
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
        F.col("ss.revenue"),
    )
    .orderBy("launch_date")
)
