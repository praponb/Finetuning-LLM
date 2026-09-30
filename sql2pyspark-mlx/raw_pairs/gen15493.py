from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNull())
    .select("status", F.lit("payment").alias("origin"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("origin")))
    .orderBy("origin")
)
