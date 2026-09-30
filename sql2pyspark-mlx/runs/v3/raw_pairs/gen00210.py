from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"))
)
