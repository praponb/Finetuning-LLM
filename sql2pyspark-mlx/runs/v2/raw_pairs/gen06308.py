from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") & F.col("balance").between(37000, 48000))
)

status_summary = (
    src
    .groupBy("status")
    .agg(F.min("balance").alias("min_balance"))
)

result = (
    status_summary
    .filter(F.col("min_balance") > 53000)
    .select("status", "min_balance")
    .orderBy(F.col("min_balance").desc(), "status")
    .limit(1)
)
