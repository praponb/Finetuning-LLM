from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select("status")
    .intersect(
        df_payments
        .filter(F.col("order_id").isin([7, 23, 34]))
        .select("status")
    )
)
