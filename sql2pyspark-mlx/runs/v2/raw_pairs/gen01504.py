from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([8, 10, 16, 19]))
    .select("status")
    .unionByName(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
        .select("status")
    )
)
