from pyspark.sql import functions as F

result = (
    df_payments
    .select("method", "order_id")
    .subtract(
        df_payments
        .filter(F.col("status") != "FAILED")
        .select("method", "order_id")
    )
)
