from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .select("channel")
    .unionByName(df_orders.filter(F.col("shipping_fee") >= 33).select("channel"))
    .orderBy(F.col("channel").desc())
)
