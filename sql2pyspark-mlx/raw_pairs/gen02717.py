from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date", "city", F.lower("country").alias("country_lower"))
)
