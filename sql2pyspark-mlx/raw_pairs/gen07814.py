from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .groupBy(F.col("s.region"))
    .agg(F.max(F.col("ss.revenue")).alias("max_revenue"))
    .orderBy("max_revenue", "region")
    .limit(5)
)
