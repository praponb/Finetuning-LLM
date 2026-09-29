from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.stock_qty").between(341, 388))
    .groupBy(F.col("p.category"))
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
    )
)
