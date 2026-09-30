from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
    )
    .filter(F.col("stddev_revenue") >= 32)
    .orderBy(F.col("stddev_revenue").desc())
)
