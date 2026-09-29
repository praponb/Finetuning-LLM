from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("ss.units").between(12, 39) & (F.col("ss.revenue") >= 1500))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
    )
)
