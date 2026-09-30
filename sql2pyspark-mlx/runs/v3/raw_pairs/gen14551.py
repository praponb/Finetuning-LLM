from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1450)
    .select("status", "method")
    .subtract(
        df_payments
        .filter(F.col("method") == "bank_transfer")
        .select("status", "method")
    )
)
