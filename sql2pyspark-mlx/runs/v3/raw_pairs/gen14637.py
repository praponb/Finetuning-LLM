from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "is_flagged",
        F.when(F.col("merchant").isin(["Lazada", "Grab"]), "tier_1").otherwise("other").alias("merchant_group"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 63)
    .orderBy("merchant_group")
)
