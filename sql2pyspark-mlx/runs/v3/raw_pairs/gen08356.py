from pyspark.sql import functions as F

src = df_transactions.filter(F.col("txn_type") == "CREDIT")

agg = (
    src
    .groupBy("merchant")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = agg.select("merchant", "avg_amount")
