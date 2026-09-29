from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter((F.col("ss.units") >= 39) & F.array_contains(F.col("p.tags"), "eco"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("p.cost"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
    )
)
