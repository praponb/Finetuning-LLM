from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]) & (F.col("account_type") == "CHECKING"))
    .groupBy("account_type")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.count("balance").alias("balance_count"),
    )
    .orderBy("account_type")
    .limit(50)
)
