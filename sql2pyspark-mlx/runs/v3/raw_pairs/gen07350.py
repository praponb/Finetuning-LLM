from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .subtract(df_customers.select("signup_date"))
    .orderBy(F.col("start_date").desc())
)
