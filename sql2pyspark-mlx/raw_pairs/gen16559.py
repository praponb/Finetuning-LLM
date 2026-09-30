from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Tokyo")
    .groupBy("city")
    .agg(
        F.count("age").alias("age_count"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
    )
)
