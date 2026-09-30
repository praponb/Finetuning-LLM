from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") < "2024-04-01") & F.col("is_active"))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)
