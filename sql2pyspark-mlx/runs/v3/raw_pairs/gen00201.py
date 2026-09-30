from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "LOAN")
    .select("status")
    .subtract(df_payments.filter(F.col("method").isNotNull()).select("status"))
)
