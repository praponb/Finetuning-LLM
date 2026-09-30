from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .groupBy("city")
    .agg(F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"))
)
