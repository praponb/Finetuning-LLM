from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .groupBy(
        F.when(F.col("s.sqft") >= 13422, "high").when(F.col("s.sqft") >= 3945, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max(F.col("ss.units")).alias("max_units"),
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
    )
    .orderBy("sqft_bucket")
    .limit(50)
)
