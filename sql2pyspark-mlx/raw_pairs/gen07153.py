from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .select(
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
    )
    .orderBy("revenue", "units")
)
