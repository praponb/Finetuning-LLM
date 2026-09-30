from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 26)
    .groupBy(
        F.hour("paid_at").alias("paid_hour"),
        F.quarter("paid_at").alias("paid_quarter"),
        "status",
    )
    .agg(F.min("paid_at").alias("min_paid_at"))
    .orderBy("status")
)
