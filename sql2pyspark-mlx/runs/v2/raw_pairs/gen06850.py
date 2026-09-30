from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .subtract(df_payments.select("status"))
    .orderBy(F.col("status").desc())
)
