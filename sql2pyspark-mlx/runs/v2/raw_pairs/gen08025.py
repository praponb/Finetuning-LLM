from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"))
    .orderBy(F.col("city").desc())
)
