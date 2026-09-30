from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("p.cost").isNotNull())
    .groupBy(
        F.when(F.col("p.stock_qty") >= 412, "high").when(F.col("p.stock_qty") >= 114, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
    .filter(F.col("revenue_range") >= 27)
    .orderBy("stock_qty_bucket")
)
