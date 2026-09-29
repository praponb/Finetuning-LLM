from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("balance").between(20000, 60000) & (F.col("opened_date") <= "2024-05-01")) | F.col("status").isin(["FROZEN", "OPEN"]))
)

account_type_summary = (
    valid_accounts
    .groupBy("account_type", "status")
    .agg(F.avg("balance").alias("avg_balance"))
)

result = account_type_summary.orderBy(F.col("status").desc())
