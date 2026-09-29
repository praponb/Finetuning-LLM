from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("email") < 8) | (F.col("segment") != "SMB"))
    .select("id", "age", "country", "signup_date")
    .orderBy("signup_date")
)
