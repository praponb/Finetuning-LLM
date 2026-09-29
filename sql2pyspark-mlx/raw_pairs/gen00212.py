from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "right")
    .groupBy(
        F.when(F.col("s.sqft") >= 15239, "high").when(F.col("s.sqft") >= 5080, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg((F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"))
    .filter(F.col("units_range") >= 58)
    .orderBy("sqft_bucket")
    .limit(5)
)
