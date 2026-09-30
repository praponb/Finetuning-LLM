from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "CREDIT"]) & F.col("opened_date").between("2023-04-01", "2025-01-12"))
)

opened_quarter_summary = (
    filtered_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = (
    opened_quarter_summary
    .select("opened_quarter", "avg_balance")
    .orderBy(F.col("avg_balance").desc())
)
