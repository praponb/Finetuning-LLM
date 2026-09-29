from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") != "Tokyo") & F.col("email").like("%@outlook.com"))
    .select("id", "signup_date")
)
