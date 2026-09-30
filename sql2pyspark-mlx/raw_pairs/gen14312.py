from pyspark.sql import functions as F

valid_customers = df_customers.filter(~F.col("is_active"))

customers_stats = (
    valid_customers
    .groupBy("segment")
    .agg(
        F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
)

result = (
    customers_stats
    .filter(F.col("bangkok_count") < 4)
    .orderBy("segment")
)
