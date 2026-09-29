from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([3, 7, 12]))
    .select("email", "country", "is_active")
)
