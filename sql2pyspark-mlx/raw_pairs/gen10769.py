from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant") == "Starbucks")
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("avg_amount") < 1100)
)
