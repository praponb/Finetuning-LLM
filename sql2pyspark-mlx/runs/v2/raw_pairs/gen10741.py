from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.count("*").alias("num_store_sales"),
    )
    .orderBy("num_store_sales", "city", "region")
    .limit(5)
)
