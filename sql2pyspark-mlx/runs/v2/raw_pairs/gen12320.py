from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("segment") != "SMB") & (F.col("signup_date") < "2024-07-01"))
    .orderBy("signup_date")
)
