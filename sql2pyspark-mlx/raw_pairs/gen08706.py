from pyspark.sql import functions as F

result = (
    df_transactions
    .cube(
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
        F.hour("txn_time").alias("txn_hour"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("amount")).otherwise(0)).alias("starbucks_amount"),
    )
    .orderBy("txn_cohort")
)
