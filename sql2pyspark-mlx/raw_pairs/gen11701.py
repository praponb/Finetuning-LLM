from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "FROZEN")
    .select("status")
    .intersect(df_payments.select("status"))
    .orderBy("status")
)
