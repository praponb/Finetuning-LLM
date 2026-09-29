from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "UK")
    .select("email", F.date_sub("signup_date", 11).alias("signup_date_minus_11d"))
)
