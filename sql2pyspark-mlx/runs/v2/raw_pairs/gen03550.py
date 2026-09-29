from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"))
    .filter(F.col("london_count") > 37)
    .select("city", "london_count")
)
