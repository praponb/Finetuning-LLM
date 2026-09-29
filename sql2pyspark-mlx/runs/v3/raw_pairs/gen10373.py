from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["SMB", "Consumer"]))
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.max("age").alias("max_age"),
    )
)
