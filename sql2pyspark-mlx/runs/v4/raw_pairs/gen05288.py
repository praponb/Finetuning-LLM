from pyspark.sql import functions as F

result = (
    df_payments
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("carrier") != "DHL")
        .select("status", F.lit("shipment").alias("source"))
    )
    .distinct()
)
