from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([13, 14, 33]))
    .select("status")
    .subtract(df_payments.select("status"))
    .orderBy(F.col("status").desc())
)
