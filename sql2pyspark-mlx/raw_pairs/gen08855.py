from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .groupBy("status")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("stddev_amount") < 34)
)
