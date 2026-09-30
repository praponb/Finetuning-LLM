from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN"]))
    .withColumn("status_group", F.when(F.col("status").isin(["FROZEN", "OPEN"]), "Priority").otherwise("Other"))
)

summary = (
    filtered_accounts
    .groupBy("account_type")
    .agg(F.count("balance").alias("balance_count"))
)

result = summary.filter(F.col("balance_count") > 6)
