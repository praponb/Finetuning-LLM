from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "age", "signup_date", "country")
    .orderBy(F.col("signup_date").desc())
)
