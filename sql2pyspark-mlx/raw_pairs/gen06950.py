from pyspark.sql import functions as F

base = df_transactions.filter(F.col("merchant") == "Grab")

agg = (
    base
    .groupBy(
        "txn_type",
        F.when(F.col("amount") >= 3600, "high").when(F.col("amount") >= 600, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.countDistinct("amount").alias("unique_merchants"))
)

result = agg.select("txn_type", "amount_bucket")
