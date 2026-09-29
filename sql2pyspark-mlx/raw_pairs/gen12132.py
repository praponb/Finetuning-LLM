from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "promptpay")
    .select("order_id", "status")
    .unionByName(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "FAILED"]))
        .select("order_id", "status")
    )
    .distinct()
    .orderBy("status")
)
