from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & F.col("email").like("%@gmail.com"))
    .select("id", "is_active", "name", "signup_date", F.floor("age").alias("age_whole"))
)
