from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") < 53)
    .select(F.col("a.balance"), F.col("a.status"), F.col("c.signup_date"))
    .orderBy(F.col("status").desc())
)
