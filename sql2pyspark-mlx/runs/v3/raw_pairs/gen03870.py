from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.count(F.when(F.col("city") == "London", 1)).alias("london_count"))
)
