from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("opened_date") > "2023-06-01")

agg = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    agg
    .filter(F.col("total_balance") >= 355000)
    .select("account_type", "min_opened_date", "total_balance")
    .orderBy(F.col("account_type").desc())
)
