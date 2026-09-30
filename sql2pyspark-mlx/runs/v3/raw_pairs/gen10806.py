from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.quarter(F.col("s.opened_date")).alias("opened_quarter"),
        F.when(F.col("s.sqft") >= 14677, "high").when(F.col("s.sqft") >= 3443, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.min(F.col("ss.revenue")).alias("min_revenue"))
)
