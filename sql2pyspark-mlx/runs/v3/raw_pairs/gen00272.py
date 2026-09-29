from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 6, 7]))
    .select("id", "is_active")
)
