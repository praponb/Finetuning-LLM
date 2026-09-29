from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").isNotNull())
    .select("src.*", F.col("c.name"), F.col("c.age"), F.col("c.credit_limit"))
)
