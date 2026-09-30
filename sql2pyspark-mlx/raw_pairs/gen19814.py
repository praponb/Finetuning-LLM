from pyspark.sql import functions as F

result = (
    df_payments
    .select("status", F.lit("payment").alias("kind"))
    .unionByName(
        df_shipments
        .filter(F.col("status").isin(["LOST", "DELIVERED"]))
        .select("status", F.lit("shipment").alias("kind"))
    )
    .distinct()
    .orderBy("status")
)
