from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.stock_qty") == 400)
    .groupBy(
        F.when(F.col("s.sqft") >= 15218, "high").when(F.col("s.sqft") >= 7805, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
        F.sum(F.col("ss.units")).alias("total_units"),
    )
    .orderBy(F.col("sqft_bucket").desc())
    .limit(5)
)
