from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2025-06-01")
    .select("status")
    .unionByName(df_payments.select("status"))
)
