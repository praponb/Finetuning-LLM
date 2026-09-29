from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"))
)
