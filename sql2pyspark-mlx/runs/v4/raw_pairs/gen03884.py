from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "merchant",
        F.when(F.year("txn_time") < 2024, "before_2024").when(F.year("txn_time") == 2024, "2024").otherwise("after_2024").alias("txn_cohort"),
    )
    .agg(F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"))
    .orderBy("lazada_count")
)
