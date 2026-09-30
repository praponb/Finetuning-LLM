from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.sum("amount").alias("total_amount"),
        F.countDistinct("amount").alias("unique_merchants"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("stddev_amount").desc(), "total_amount")
)
