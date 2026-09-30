from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() | F.col("account_type").isin(["SAVINGS", "LOAN"]))
    .groupBy("account_type", "status")
    .agg(
        F.max("balance").alias("max_balance"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
    )
)
