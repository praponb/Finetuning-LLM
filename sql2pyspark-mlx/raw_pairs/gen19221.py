from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.country"))
    .agg(F.count(F.col("a.id")).alias("account_count"))
    .filter(F.col("account_count") > 1)
    .orderBy("account_count")
)
