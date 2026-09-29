from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.3)
    .select("status")
    .unionByName(df_accounts.select("status"))
)
