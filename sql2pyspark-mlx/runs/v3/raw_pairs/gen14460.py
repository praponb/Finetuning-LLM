from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.col("c.city"))
    .agg(F.count(F.col("a.id")).alias("account_count"))
    .filter(F.col("account_count") > 4)
    .orderBy("city", "account_count")
    .limit(3)
)
