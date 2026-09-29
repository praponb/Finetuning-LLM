from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") | F.col("order_id").isNotNull())
    .select(
        "id",
        "method",
        "paid_at",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "Priority").otherwise("Other").alias("status_group"),
        F.date_trunc("year", "paid_at").alias("paid_at_period"),
    )
    .orderBy("paid_at")
)
