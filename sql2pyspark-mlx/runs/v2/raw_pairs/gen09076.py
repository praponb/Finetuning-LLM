from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left_semi")
    .filter(F.col("c.signup_date") >= "2024-08-25")
    .select(F.col("c.id"), F.col("c.name"), F.col("c.email"), F.col("c.country"))
)
