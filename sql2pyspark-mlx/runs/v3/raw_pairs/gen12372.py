from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["SUCCESS", "REFUNDED"]))
    .select(
        "id",
        "status",
        "method",
        F.date_trunc("week", "paid_at").alias("paid_at_period"),
    )
    .orderBy(F.col("method").desc())
)
