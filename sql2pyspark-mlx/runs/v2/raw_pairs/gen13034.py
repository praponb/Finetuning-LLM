from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") != "UK") & F.col("city").isin(["London", "Singapore"]))
    .select("id", "city", F.date_sub("signup_date", 14).alias("signup_date_minus_14d"))
)
