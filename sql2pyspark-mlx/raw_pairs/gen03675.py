from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.lit("payment").alias("source"), "status")
    .unionByName(df_shipments.select(F.lit("shipment").alias("source"), "status"))
    .distinct()
    .orderBy("status")
)
