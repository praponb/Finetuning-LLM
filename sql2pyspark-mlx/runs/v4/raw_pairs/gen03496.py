from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 26)
    .select("status")
    .subtract(df_accounts.filter(F.col("status") != "FROZEN").select("status"))
    .orderBy("status")
)
