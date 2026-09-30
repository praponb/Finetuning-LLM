from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("status").isin(["LOST", "DELIVERED", "IN_TRANSIT"]))
        .select("status", F.lit("shipment").alias("source"))
    )
    .orderBy("source")
)
