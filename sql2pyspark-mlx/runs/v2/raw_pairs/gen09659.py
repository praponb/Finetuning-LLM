from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2023-09-01") & F.col("amount").isNull())
    .select(
        "id",
        "account_id",
        F.ceil("amount").alias("amount_whole"),
        F.when(F.col("merchant").isin(["Amazon", "Grab"]), "Tier 1").otherwise("Other").alias("merchant_group"),
    )
    .orderBy(F.col("id").desc(), "account_id")
    .limit(5)
)
