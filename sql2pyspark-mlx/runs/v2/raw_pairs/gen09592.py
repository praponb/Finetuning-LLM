from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("p.cost"),
        (F.col("ss.revenue") * F.col("ss.units")).alias("revenue_per_unit"),
    )
)
