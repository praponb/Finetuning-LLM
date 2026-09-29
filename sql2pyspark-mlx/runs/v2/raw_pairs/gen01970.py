from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(40, 67) | F.month("signup_date").isin([7, 8, 9]))
    .select("id", "credit_limit", "name")
)
