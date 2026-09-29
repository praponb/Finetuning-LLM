from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.month(F.col("p.launch_date")).isin([2, 9, 12]))
    .groupBy(F.col("p.category"))
    .agg(
        F.min(F.col("ss.revenue")).alias("min_revenue"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .filter(F.col("min_revenue") >= 2400)
    .orderBy(F.col("category").desc())
)
