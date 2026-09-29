from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .groupBy(
        F.month(F.col("s.opened_date")).alias("opened_month"),
        F.when(F.col("s.sqft") >= 14622, "high").when(F.col("s.sqft") >= 6866, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max(F.col("ss.revenue")).alias("max_revenue"))
    .orderBy("max_revenue")
)
