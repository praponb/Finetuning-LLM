from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4100, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
        "merchant",
    )
    .agg(F.countDistinct("amount").alias("unique_accounts"))
    .filter(F.col("unique_accounts") >= 5)
    .select("amount_bucket", "merchant", "unique_accounts")
    .orderBy("merchant", F.col("unique_accounts").desc())
)
