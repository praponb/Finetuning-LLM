from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .groupBy("is_active", "segment")
    .agg(F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"))
    .filter(F.col("uk_count") < 4)
    .select("is_active", "segment", "uk_count")
    .orderBy(F.col("uk_count").desc())
)
