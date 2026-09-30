from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count("amount").alias("amount_count"),
        F.sum("amount").alias("total_amount"),
    )
    .orderBy("status", "total_amount")
)
