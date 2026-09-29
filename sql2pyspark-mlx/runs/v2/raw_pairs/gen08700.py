from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Grab", "Amazon", "Lazada"]) & F.hour("txn_time").between(12, 19))
)

agg = (
    recent_transactions
    .groupBy(
        F.when(F.col("amount") >= 4500, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
        "merchant",
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
)

result = agg.filter(F.col("max_amount") >= 2900)
