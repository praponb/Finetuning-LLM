from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((F.col("amount") >= 1600) & F.col("is_flagged")))
    .groupBy("txn_type")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
    )
    .orderBy(F.col("grab_amount").desc())
)
