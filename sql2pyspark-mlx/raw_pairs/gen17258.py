from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@outlook.com") & F.col("signup_date").between("2024-02-18", "2024-12-01"))
    .select("id", "signup_date", "city")
)
