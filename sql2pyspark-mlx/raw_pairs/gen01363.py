from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("txn_type") == "FEE")

txn_cohort_summary = (
    filtered_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    txn_cohort_summary
    .filter(F.col("total_amount") < 16000)
    .orderBy(F.col("total_amount").desc(), "txn_cohort")
    .limit(5)
)
