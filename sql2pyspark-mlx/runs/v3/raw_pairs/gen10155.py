from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("status") == "OPEN") & F.col("balance").between(64000, 78000))
    .withColumn("status_group", F.when(F.col("status").isin(["OPEN", "CLOSED"]), "Group A").otherwise("Other"))
)

accounts_stats = (
    valid_accounts
    .groupBy("account_type")
    .agg(F.sum("balance").alias("total_balance"))
)

result = (
    accounts_stats
    .select("account_type", "total_balance")
    .orderBy("account_type", F.col("total_balance").desc())
)
