from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(df_payments.select("status"))
)
