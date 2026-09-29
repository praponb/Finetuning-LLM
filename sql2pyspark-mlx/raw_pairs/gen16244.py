from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select(
        "id",
        "method",
        "order_id",
        "paid_at",
        F.when(F.col("status").isin(["REFUNDED", "SUCCESS"]), "Group A").otherwise("Other").alias("status_group"),
    )
)
