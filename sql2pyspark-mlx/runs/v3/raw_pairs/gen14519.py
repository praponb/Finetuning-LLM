from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("merchant").isin(["7-Eleven", "Amazon"]), "group_a").otherwise("other").alias("merchant_group"),
    )
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)
