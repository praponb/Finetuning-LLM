from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("checking_balance") >= 61)
    .select("account_type", "checking_balance", "min_opened_date")
)
