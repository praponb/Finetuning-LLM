from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") == 2024)
    .select("city", F.length("country").alias("country_length"))
)
