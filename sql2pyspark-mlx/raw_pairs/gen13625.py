from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.signup_date").between("2023-07-01", "2023-08-01"))
    .select(F.col("a.id"), F.col("a.account_type"), F.col("c.country"))
)
