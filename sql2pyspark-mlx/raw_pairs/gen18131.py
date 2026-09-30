from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("merchant").isin(["7-Eleven", "Grab"]), "priority").otherwise("other").alias("merchant_group"),
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") < 1400)
)
