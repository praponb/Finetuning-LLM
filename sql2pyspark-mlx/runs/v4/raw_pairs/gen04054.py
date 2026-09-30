from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "bank_transfer")
    .groupBy(
        F.when(F.col("status").isin(["SUCCESS", "REFUNDED"]), "group_a").otherwise("other").alias("status_group"),
        "method",
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
    )
)
