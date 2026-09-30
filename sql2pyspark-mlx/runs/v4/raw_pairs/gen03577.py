from pyspark.sql import functions as F

result = (
    df_payments
    .select("method")
    .exceptAll(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
        .select("method")
    )
)
