from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "CHECKING"]))
    .groupBy("account_type")
    .agg(
        F.count("*").alias("account_count"),
        F.max("balance").alias("max_balance"),
    )
    .filter(F.col("account_count") < 2)
    .orderBy("max_balance")
)
