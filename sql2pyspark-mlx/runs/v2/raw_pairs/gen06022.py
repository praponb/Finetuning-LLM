from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .select("channel")
    .unionByName(df_orders.filter(F.col("amount").between(960, 1400)).select("channel"))
    .distinct()
    .orderBy(F.col("channel").desc())
)
