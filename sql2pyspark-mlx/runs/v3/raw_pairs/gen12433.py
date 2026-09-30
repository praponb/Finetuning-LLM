from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "order_id",
        F.when(F.col("status").isin(["REFUNDED", "SUCCESS"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
    )
)
