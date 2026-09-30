from pyspark.sql import functions as F

src = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() | F.col("account_type").isin(["CREDIT", "LOAN", "SAVINGS"]))
)

account_type_summary = (
    src
    .groupBy("account_type")
    .agg(F.min("balance").alias("min_balance"))
)

result = (
    account_type_summary
    .filter(F.col("min_balance") >= 29000)
    .select("account_type", "min_balance")
    .orderBy("account_type")
)
