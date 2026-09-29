from pyspark.sql import functions as F

src = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS"]) & ((F.col("opened_date") >= "2025-09-26") | F.col("balance").between(44000, 56000)))
)

summary = (
    src
    .groupBy("status")
    .agg(F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"))
)

result = (
    summary
    .orderBy(F.col("loan_count").desc(), "status")
    .limit(50)
)
