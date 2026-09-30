from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(~F.col("city").isin(["Berlin", "Bangkok", "Chiang Mai"]) & (F.col("sqft") <= 6468))
)

stores_stats = (
    filtered_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"))
)

result = stores_stats
