from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("method").isin(["card", "cash", "bank_transfer"]))
        .select("status")
    )
    .orderBy("status")
)
