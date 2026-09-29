from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("origin"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("origin")))
    .distinct()
    .orderBy(F.col("origin").desc())
)
