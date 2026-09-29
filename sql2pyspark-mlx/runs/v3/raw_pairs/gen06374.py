from pyspark.sql import functions as F

result = (
    df_accounts
    .cube("account_type", F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.count("balance").alias("balance_count"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_count"),
    )
)
