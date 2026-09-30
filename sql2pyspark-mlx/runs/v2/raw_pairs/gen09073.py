from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2023-05-21")
    .agg(
        F.count("amount").alias("amount_count"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
)
