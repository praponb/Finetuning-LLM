from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .withColumn("status_length", F.length("status"))
)

summary = (
    base
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
    )
)

result = (
    summary
    .filter(F.col("credit_balance") < 34)
    .select("account_type", "credit_balance")
)
