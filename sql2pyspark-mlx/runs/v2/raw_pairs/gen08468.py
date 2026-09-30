from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.id"))
    .agg(F.count(F.col("a.id")).alias("account_count"))
    .orderBy("account_count", F.col("id").desc())
)
