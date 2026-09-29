from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") > 74000) | ~F.col("account_type").isin(["CREDIT", "SAVINGS", "LOAN"]))
    .groupBy("status")
    .agg(F.min("balance").alias("min_balance"))
    .filter(F.col("min_balance") >= 23000)
    .orderBy(F.col("min_balance").desc(), "status")
    .limit(3)
)
