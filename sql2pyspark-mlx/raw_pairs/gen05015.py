from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.sqft") >= 14761, "high").when(F.col("s.sqft") >= 4343, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max(F.col("ss.sale_date")).alias("last_sale_date"))
)
