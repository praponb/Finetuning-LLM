from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(F.col("p.supplier_id").isin([13, 30]))
    .groupBy(F.col("p.category"))
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
    )
    .orderBy("category")
)
