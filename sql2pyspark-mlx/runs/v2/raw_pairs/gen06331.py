from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "store")
    .select("status")
    .unionByName(df_payments.select("status"))
    .distinct()
    .orderBy(F.col("status").desc())
)
