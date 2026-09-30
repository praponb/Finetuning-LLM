from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "SUCCESS"]))
    .select("id", "status")
    .subtract(
        df_payments
        .filter(F.col("method") == "bank_transfer")
        .select("id", "status")
    )
    .orderBy(F.col("status").desc())
)
