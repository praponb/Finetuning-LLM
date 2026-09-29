from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "txn_type",
        F.when(F.col("merchant").isin(["Grab", "7-Eleven"]), "priority").otherwise("other").alias("merchant_group"),
    )
    .agg(F.countDistinct("amount").alias("unique_accounts"))
    .filter(F.col("unique_accounts") > 4)
    .orderBy(F.col("txn_type").desc(), "unique_accounts", "merchant_group")
    .limit(3)
)
