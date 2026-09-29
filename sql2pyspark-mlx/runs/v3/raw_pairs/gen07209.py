from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
    .select("status", F.lit("payment").alias("source"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("source")))
    .distinct()
    .orderBy(F.col("source").desc())
)
