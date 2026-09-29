from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "amount",
        F.substring("method", 1, 2).alias("method_prefix"),
        F.concat("method", F.lit(", "), "status").alias("method_status"),
    )
)
