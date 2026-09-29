from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("balance").isNull() & ~F.col("status").isin(["OPEN", "FROZEN"]) & (F.col("balance") > 79000))
)

account_type_summary = (
    recent_accounts
    .groupBy("account_type")
    .agg(F.countDistinct("balance").alias("unique_statuss"))
)

result = (
    account_type_summary
    .filter(F.col("unique_statuss") < 5)
    .orderBy("account_type")
    .limit(20)
)
