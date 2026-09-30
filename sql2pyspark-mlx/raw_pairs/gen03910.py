from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") < 50000) & F.col("account_type").isin(["CHECKING", "CREDIT", "LOAN"]))
    .groupBy("account_type")
    .agg(F.max("balance").alias("max_balance"))
    .filter(F.col("max_balance") >= 27000)
    .orderBy(F.col("account_type").desc(), F.col("max_balance").desc())
    .limit(5)
)
