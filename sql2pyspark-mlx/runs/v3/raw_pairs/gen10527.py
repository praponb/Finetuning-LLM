from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 440) & F.col("order_id").isin([12, 37]))
    .select(
        "method",
        "status",
        F.substring("status", 1, 2).alias("status_prefix"),
        F.concat_ws("-", "method", "status").alias("method_status_key"),
    )
    .orderBy("status_prefix", F.col("method_status_key").desc())
)
