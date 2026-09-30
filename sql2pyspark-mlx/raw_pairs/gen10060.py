from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .select("method", "order_id")
    .unionByName(
        df_payments
        .filter(F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
        .select("method", "order_id")
    )
    .distinct()
)
