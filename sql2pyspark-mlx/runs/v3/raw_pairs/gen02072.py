from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy("avg_amount")
)
