from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["REFUNDED", "FAILED"]))
    .select(
        "id",
        "status",
        "method",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
    )
    .orderBy("id")
    .limit(10)
)
