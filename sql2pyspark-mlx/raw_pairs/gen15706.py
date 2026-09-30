from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(df_accounts.select("status"))
    .distinct()
)
