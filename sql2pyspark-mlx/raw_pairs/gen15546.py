from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .groupBy(
        F.when(F.col("status").isin(["OPEN", "CLOSED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
    )
    .orderBy("status_group")
    .limit(1)
)
