from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Starbucks", "Grab"]))
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0)).alias("lazada_count"),
        F.countDistinct("amount").alias("unique_merchants"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("unique_merchants").desc(), "lazada_count")
)
