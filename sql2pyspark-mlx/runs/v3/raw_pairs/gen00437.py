from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("status", F.lit("order").alias("source"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("source")))
    .orderBy(F.col("source").desc())
)
