from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy(F.col("total_amount").desc(), F.col("txn_cohort").desc())
    .limit(1)
)
