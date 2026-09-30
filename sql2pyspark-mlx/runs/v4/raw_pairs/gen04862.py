from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.supplier_id") == 28)
    .groupBy(
        F.when(F.col("p.stock_qty") >= 421, "high").when(F.col("p.stock_qty") >= 52, "mid").otherwise("low").alias("stock_qty_bucket"),
        F.col("p.category"),
    )
    .agg(F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"))
    .orderBy("stddev_revenue", F.col("stock_qty_bucket").desc(), "category")
    .limit(10)
)
