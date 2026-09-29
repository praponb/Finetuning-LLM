from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2024-11-05") & ~F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count("amount").alias("amount_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 1800)
    .drop("min_amount")
    .orderBy("merchant", F.col("amount_count").desc())
    .limit(20)
)
