from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") < 7300) & ((F.col("signup_date") >= "2023-06-01") | ~F.col("city").isin(["Singapore", "Berlin", "Bangkok"])))
    .select("id", "city", "email", "age")
)
