from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(810, 1480))
    .select("status")
    .unionByName(df_subscriptions.select("status"))
    .distinct()
    .orderBy(F.col("status").desc())
)
