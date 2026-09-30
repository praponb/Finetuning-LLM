from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["SG", "UK"]))
    .select("id", "signup_date")
    .subtract(df_customers.filter(F.col("age") >= 60).select("id", "signup_date"))
    .orderBy("id")
)
