from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.19, 0.27))
    .select("channel")
    .unionByName(df_orders.filter(F.col("amount") >= 750).select("channel"))
    .orderBy("channel")
)
