from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 56000)
    .select("status")
    .unionByName(df_shipments.select("status"))
    .distinct()
)
