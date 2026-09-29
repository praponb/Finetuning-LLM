from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("merchant").isin(["7-Eleven", "Starbucks"]), "tier_1").otherwise("other").alias("merchant_group"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc(), F.col("amount_count").desc())
)
