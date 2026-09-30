from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.email"), F.col("b.signup_date"))
)
