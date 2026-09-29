from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3200, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
        "account_id",
        "merchant",
    )
    .agg(F.countDistinct("amount").alias("unique_accounts"))
    .orderBy(F.col("merchant").desc())
)
