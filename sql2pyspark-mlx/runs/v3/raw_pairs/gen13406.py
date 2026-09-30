from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4100, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.countDistinct("amount").alias("unique_merchants"))
    .filter(F.col("unique_merchants") < 8)
    .select("amount_bucket", "txn_cohort", "unique_merchants")
    .orderBy(F.col("unique_merchants").desc(), "amount_bucket", "txn_cohort")
    .limit(10)
)
