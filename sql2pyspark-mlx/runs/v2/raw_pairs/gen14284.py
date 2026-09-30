from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(df_accounts.filter(F.col("balance") <= 30000).select("status"))
    .orderBy("status")
)
