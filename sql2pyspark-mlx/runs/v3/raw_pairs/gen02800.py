from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("balance") < 78000) | (F.col("opened_date") >= "2024-02-01"))
    .withColumn("account_type_prefix", F.substring("account_type", 1, 4))
)

account_type_summary = (
    valid_accounts
    .groupBy("account_type", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.avg("balance").alias("avg_balance"))
)

result = (
    account_type_summary
    .select("account_type", "opened_month")
    .orderBy(F.col("account_type").desc())
)
