from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
        F.to_date("txn_time").alias("txn_day"),
    )
    .agg(F.countDistinct("amount").alias("unique_accounts"))
    .orderBy(F.col("unique_accounts").desc())
)
