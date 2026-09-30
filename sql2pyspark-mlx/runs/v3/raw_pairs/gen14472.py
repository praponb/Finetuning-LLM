from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1420)
    .select("status")
    .unionByName(df_orders.filter(F.col("amount") > 600).select("status"))
    .orderBy("status")
)
