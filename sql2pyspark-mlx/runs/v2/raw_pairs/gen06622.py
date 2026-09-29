from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.29, 0.31))
    .select("status")
    .unionByName(df_accounts.select("status"))
    .orderBy("status")
)
