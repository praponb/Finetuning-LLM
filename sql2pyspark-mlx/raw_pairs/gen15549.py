from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("status") == "OPEN") & (F.col("opened_date") >= "2023-09-01"))
)

agg = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.max("opened_date").alias("last_opened_date"),
    )
)

result = (
    agg
    .filter(F.col("avg_balance") > 40)
    .orderBy("account_type", "avg_balance")
)
