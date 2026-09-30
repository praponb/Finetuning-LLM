from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .select("signup_date")
    .union(df_subscriptions.select("start_date"))
)
