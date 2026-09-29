from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "account_id",
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
    .filter(F.col("unique_accounts") < 7)
)
