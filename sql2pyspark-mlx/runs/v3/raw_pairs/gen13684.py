from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("amount") <= 3600)
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

agg = (
    recent_transactions
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", 1).otherwise(0)).alias("amazon_count"),
    )
)

result = (
    agg
    .select("merchant", "amazon_count")
    .orderBy(F.col("amazon_count").desc(), "merchant")
    .limit(1)
)
