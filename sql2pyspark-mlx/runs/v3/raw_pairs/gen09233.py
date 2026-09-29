from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "SUCCESS"]))
    .select("id", "method")
    .unionByName(
        df_payments
        .filter(F.to_date("paid_at") < "2024-01-23")
        .select("id", "method")
    )
    .orderBy(F.col("method").desc())
)
