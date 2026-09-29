from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select("id", "signup_date")
    .orderBy(F.col("signup_date").desc())
)
