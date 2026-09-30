from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        "merchant",
        F.when(F.col("amount") >= 3900, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount"))).alias("amazon_total_amount"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "7-Eleven", 1)), F.lit(0)), 2).alias("amount_per_7_eleven"),
        F.max(F.when(F.col("merchant") == "7-Eleven", F.col("amount"))).alias("7_eleven_max_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("amazon_total_amount").desc_nulls_last())
)
