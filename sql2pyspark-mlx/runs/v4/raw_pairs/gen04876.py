from pyspark.sql import functions as F

result = (
    df_payments
    .select("method", "status")
    .subtract(
        df_payments
        .filter(F.col("status").isin(["FAILED", "SUCCESS"]))
        .select("method", "status")
    )
)
