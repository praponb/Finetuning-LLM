from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.col("p.category") == "Grocery")
    .groupBy(
        F.col("s.city"),
        F.when(F.col("s.sqft") >= 14872, "high").when(F.col("s.sqft") >= 2863, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc(), "city", "sqft_bucket")
    .limit(3)
)
