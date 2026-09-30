from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "DE")
    .groupBy("city")
    .agg(F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"))
    .orderBy("city")
)
