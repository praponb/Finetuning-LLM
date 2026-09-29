from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "SMB")
    .select("id", "city", "signup_date", "email")
)
