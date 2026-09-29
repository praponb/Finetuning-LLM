from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4500, "high").when(F.col("amount") >= 900, "mid").otherwise("low").alias("amount_bucket"),
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("avg_amount").desc(), "amount_bucket")
)
