from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter((F.col("p.stock_qty") == 125) & F.col("ss.units").between(32, 33))
    .select(
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("p.stock_qty"),
    )
)
