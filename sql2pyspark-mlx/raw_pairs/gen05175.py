from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
    .filter(F.col("closed_count") > 41)
    .orderBy(F.col("closed_count").desc(), "account_type")
    .limit(5)
)
