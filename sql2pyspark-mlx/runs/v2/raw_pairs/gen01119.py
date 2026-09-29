from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "amount",
        "method",
        F.concat("method", F.lit(", "), "status").alias("method_status"),
        F.length("method").alias("method_length"),
    )
)
