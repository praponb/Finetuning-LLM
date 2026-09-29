from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("segment") != "Corporate") & (F.col("city") == "New York"))
)

agg = (
    filtered_customers
    .groupBy("segment", "is_active")
    .agg(F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"))
)

result = (
    agg
    .filter(F.col("berlin_count") > 10)
    .orderBy("segment", "is_active")
    .limit(10)
)
