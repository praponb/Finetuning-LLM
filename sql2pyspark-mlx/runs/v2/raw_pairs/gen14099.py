from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(35, 38))
    .select("status")
    .unionByName(df_accounts.filter(F.col("status") != "OPEN").select("status"))
    .orderBy(F.col("status").desc())
)
