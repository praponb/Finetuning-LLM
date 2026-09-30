from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.col("opened_date").between("2023-11-28", "2025-03-01") & F.col("opened_date").between("2023-08-01", "2024-05-01"))
    .withColumn("account_type_lower", F.lower("account_type"))
)

summary = (
    valid_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)

result = (
    summary
    .filter(F.col("stddev_balance") > 34)
    .orderBy("stddev_balance", F.col("opened_month").desc())
)
