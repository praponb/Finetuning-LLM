from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") | (F.col("status") != "FAILED"))
    .select(
        "id",
        "status",
        "amount",
        "method",
        F.substring("status", 1, 3).alias("status_prefix"),
        F.hour("paid_at").alias("paid_at_hour"),
    )
)
