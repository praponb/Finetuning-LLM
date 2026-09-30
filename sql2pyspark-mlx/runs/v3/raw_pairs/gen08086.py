from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "DELIVERED")
    .select("cost", F.lit("shipment").alias("source"))
    .union(df_orders.select("amount", F.lit("order").alias("source")))
    .orderBy(F.col("source").desc())
)
