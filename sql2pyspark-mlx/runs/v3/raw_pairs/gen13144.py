from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("s.sqft") > 15560)
    .groupBy(
        F.when(F.col("p.cost") >= 670, "high").when(F.col("p.cost") >= 150, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.avg(F.col("ss.revenue")).alias("avg_revenue"))
    .orderBy("cost_bucket")
)
