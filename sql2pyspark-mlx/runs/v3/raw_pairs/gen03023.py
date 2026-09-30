from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(F.col("sqft").between(7526, 12804) & F.col("store_name").isNotNull()))
    .agg(F.count(F.when(F.col("region") == "East", 1)).alias("east_count"))
)
