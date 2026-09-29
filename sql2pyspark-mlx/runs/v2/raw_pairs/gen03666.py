from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(
        F.when(F.col("p.stock_qty") >= 444, "high").when(F.col("p.stock_qty") >= 123, "mid").otherwise("low").alias("stock_qty_bucket"),
        F.col("p.brand"),
    )
    .agg(
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
    .filter(F.col("revenue_range") > 55)
    .orderBy("brand")
)
