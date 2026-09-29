from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "method",
        "status",
        "amount",
        F.col("amount").cast("int").alias("amount_int"),
    )
)
