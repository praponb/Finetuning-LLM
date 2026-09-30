from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS"]) & (F.col("status") == "FAILED"))
    .groupBy("status")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.countDistinct("amount").alias("unique_orders"),
    )
    .orderBy(F.col("unique_orders").desc())
)
