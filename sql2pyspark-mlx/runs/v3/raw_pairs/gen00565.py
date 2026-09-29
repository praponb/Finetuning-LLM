from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") <= "2024-04-01")
    .select("opened_date")
    .subtract(df_customers.select(F.col("signup_date").alias("opened_date")))
)
