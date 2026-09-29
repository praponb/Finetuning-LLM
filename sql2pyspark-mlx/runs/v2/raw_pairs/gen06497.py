from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        F.concat_ws(",", "status", "method").alias("status_method_key"),
    )
)
