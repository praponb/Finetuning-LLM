from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "London")
    .groupBy("is_active", "city", "segment")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .orderBy("age_range")
)
