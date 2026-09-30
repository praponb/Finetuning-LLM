from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .subtract(
        df_customers
        .filter(F.col("credit_limit").between(6900, 8700))
        .select("signup_date")
    )
)
