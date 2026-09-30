from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2025-03-01") & (F.col("account_id").isin([16, 37, 39]) | F.col("merchant").isNotNull()))
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)
