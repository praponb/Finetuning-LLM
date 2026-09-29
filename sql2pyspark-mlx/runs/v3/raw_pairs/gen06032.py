from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(F.count(F.col("a.id")).alias("account_count"))
    .orderBy("country", F.col("account_count").desc())
)
