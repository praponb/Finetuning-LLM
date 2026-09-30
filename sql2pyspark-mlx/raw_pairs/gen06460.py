from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 4)
    .select("account_type", "total_balance", "total_count")
    .orderBy(F.col("total_balance").desc())
)
