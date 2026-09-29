from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") <= 760)
    .select("method")
    .subtract(
        df_payments
        .filter(F.to_date("paid_at") >= "2024-06-01")
        .select("method")
    )
)
