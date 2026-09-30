from pyspark.sql import functions as F

result = (
    df_orders
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.col("status") != "RETURNED")
        .select("status", F.lit("shipment").alias("source"))
    )
    .distinct()
    .orderBy("status")
)
