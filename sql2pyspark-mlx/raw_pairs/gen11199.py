from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .withColumn("balance_int", F.col("balance").cast("int"))
)

summary = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance_int")).otherwise(0)).alias("checking_balance_int"),
    )
)

result = (
    summary
    .select("account_type", "checking_balance_int")
    .orderBy("checking_balance_int")
)
