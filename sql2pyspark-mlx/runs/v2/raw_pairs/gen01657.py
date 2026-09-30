from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("id", "status", "order_id")
    .subtract(
        df_payments
        .filter(~F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
        .select("id", "status", "order_id")
    )
)
