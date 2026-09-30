from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["RETURNED", "COMPLETED", "CANCELLED"]))
    .groupBy("channel")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .filter(F.col("total_shipping_fee") >= 140)
    .orderBy(F.col("total_shipping_fee").desc())
)
