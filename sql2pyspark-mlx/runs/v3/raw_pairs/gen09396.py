from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(~(F.col("customer_id").isin([18, 20, 23, 30]) | F.col("account_type").isin(["LOAN", "CREDIT"])))
)

agg = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0)).alias("loan_count"),
    )
)

result = agg.select("account_type", "total_balance", "loan_count")
