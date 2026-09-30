from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 66000)
    .select("status")
    .unionByName(df_shipments.filter(F.col("status") == "RETURNED").select("status"))
)
