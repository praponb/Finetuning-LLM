from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("balance") >= 62000)

account_type_summary = (
    recent_accounts
    .groupBy("account_type")
    .agg(
        F.count("*").alias("cnt"),
        F.max("opened_date").alias("latest_opened_date"),
    )
)

result = (
    account_type_summary
    .select("account_type", "cnt", "latest_opened_date")
    .orderBy("account_type", F.col("cnt").desc())
)
