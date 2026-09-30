from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 72000)
    .select("status")
    .subtract(df_shipments.filter(F.col("cost").between(28, 56)).select("status"))
)
