from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .groupBy(
        F.when(F.col("p.price") >= 1290, "high").when(F.col("p.price") >= 480, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.min(F.col("ss.revenue")).alias("min_revenue"),
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
    )
    .orderBy("stddev_revenue")
)
