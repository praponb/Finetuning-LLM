from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Grab"]) & F.col("amount").between(2700, 2800))
    .withColumn("merchant_prefix", F.substring("merchant", 1, 3))
)

summary = (
    src
    .groupBy("is_flagged")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = summary.select("is_flagged", "amount_range")
