from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("order_id").isin([6, 11, 16, 29]))
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
