from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select("status")
    .unionByName(df_accounts.filter(F.col("balance") < 47000).select("status"))
    .orderBy("status")
)
