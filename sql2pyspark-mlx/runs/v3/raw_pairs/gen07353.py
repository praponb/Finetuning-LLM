from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Grab", "7-Eleven"]))
    .groupBy(
        "txn_type",
        F.when(F.col("amount") >= 3000, "high").when(F.col("amount") >= 800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") < 3300)
    .orderBy(F.col("txn_type").desc())
)
