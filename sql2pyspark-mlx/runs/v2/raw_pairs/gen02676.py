from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 800)
    .select("order_id")
    .subtract(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
        .select("order_id")
    )
)
