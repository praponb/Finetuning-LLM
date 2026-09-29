from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("id", "status")
    .unionByName(df_accounts.filter(F.col("balance") >= 20000).select("id", "status"))
)
