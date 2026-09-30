from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") != "Corporate")
    .select("signup_date", F.length("city").alias("city_length"))
)
