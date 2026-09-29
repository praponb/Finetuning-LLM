from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("status").isNull() & (F.col("customer_id") == 37))

summary = (
    recent_accounts
    .groupBy("account_type")
    .agg(F.count("*").alias("num_accounts"))
)

result = (
    summary
    .select("account_type", "num_accounts")
    .orderBy("num_accounts")
)
