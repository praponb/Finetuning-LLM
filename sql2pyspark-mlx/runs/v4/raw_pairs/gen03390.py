from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .intersect(df_subscriptions.select("end_date"))
)
