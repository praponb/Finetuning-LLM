from pyspark.sql import functions as F

max_signup_date = df_customers.filter(F.col("age") < 45).agg(F.max("signup_date")).first()[0]

result = (
    df_customers
    .filter(F.col("signup_date") == max_signup_date)
    .select("id", "signup_date")
    .orderBy(F.col("signup_date").desc())
)
