from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 19) & (F.to_date("paid_at") < "2023-09-01"))
    .groupBy(
        "status",
        "method",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
)
