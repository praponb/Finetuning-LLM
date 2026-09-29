from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.category").isin(["Fashion", "Books"]))
    .groupBy(F.col("p.category"))
    .agg(F.sum(F.col("ss.revenue")).alias("total_revenue"))
)
