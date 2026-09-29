from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("opened_date") <= "2023-11-01")
    .withColumn("clean_status", F.trim("status"))
)

summary = (
    recent_accounts
    .groupBy("account_type")
    .agg(F.sum("balance").alias("total_balance"))
)

result = (
    summary
    .filter(F.col("total_balance") < 146000)
    .select("account_type", "total_balance")
    .orderBy("total_balance", F.col("account_type").desc())
)
