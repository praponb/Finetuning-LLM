from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.when(F.col("merchant").isin(["Grab", "Shell"]), "group_a").otherwise("other").alias("merchant_group"))
    .pivot("merchant", ["Grab", "Starbucks", "Shell"])
    .agg(F.avg("amount"))
)
