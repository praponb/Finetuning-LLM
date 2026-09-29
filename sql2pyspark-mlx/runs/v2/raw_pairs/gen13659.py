from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 12)
    .select("status", F.lit("order").alias("source"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("source")))
    .distinct()
)
