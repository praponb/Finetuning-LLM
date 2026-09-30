from pyspark.sql import functions as F

src = df_transactions.filter(F.col("merchant") != "Amazon")

summary = (
    src
    .groupBy("txn_type", "merchant")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    summary
    .filter(F.col("max_amount") >= 2600)
    .orderBy(F.col("max_amount").desc())
)
