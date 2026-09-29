from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 8)
    .select("status")
    .unionByName(df_accounts.filter(F.col("status") == "CLOSED").select("status"))
)
