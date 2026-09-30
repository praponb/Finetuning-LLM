from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(42000, 77000))
    .select("status")
    .intersect(df_payments.filter(F.col("method") != "cash").select("status"))
)
