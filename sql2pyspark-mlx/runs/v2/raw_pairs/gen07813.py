from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(8, 12))
    .select(
        "id",
        "status",
        "paid_at",
        "method",
        F.to_date("paid_at").alias("paid_at_date"),
        F.upper("method").alias("method_upper"),
    )
)
