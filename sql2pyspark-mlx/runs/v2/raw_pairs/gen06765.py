from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "DE") | (F.col("city") == "Chiang Mai"))
    .select("id", "signup_date", F.length("country").alias("country_length"))
)
