from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") != 39) & (F.col("signup_date") < "2023-06-01"))
    .select("id", "name", "age")
)
