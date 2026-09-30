from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 59)
    .select("id", "age", "email", "signup_date")
)
