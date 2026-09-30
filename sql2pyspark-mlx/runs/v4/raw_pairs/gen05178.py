from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(1410, 1570) & (F.col("order_id") == 19) & F.col("status").isin(["FAILED", "REFUNDED"]))
    .select(
        "id",
        "method",
        F.lower("status").alias("status_lower"),
        F.lower("method").alias("method_lower"),
    )
)
