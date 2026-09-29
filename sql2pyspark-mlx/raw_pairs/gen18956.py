from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.product_id").isin([4, 6, 7, 9]))
    .groupBy(F.col("s.region"))
    .agg(
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
    .orderBy(F.col("revenue_range").desc(), "region")
    .limit(10)
)
