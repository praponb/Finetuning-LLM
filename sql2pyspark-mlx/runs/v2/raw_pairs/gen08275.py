from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.col("account_type") == "CHECKING") & (F.col("opened_date") >= "2024-11-01"))
)

summary = (
    src
    .groupBy("account_type")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = (
    summary
    .filter(F.col("avg_balance") > 76)
    .select("account_type", "avg_balance")
    .orderBy(F.col("avg_balance").desc(), "account_type")
    .limit(5)
)
