from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "outer")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(
        F.min(F.col("ss.revenue")).alias("min_revenue"),
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
    )
    .filter(F.col("min_revenue") > 2300)
)
