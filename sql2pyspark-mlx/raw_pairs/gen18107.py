from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-09-07", "2024-04-24"))
    .select("status", F.lit("shipment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select("status", F.lit("subscription").alias("source"))
    )
    .orderBy("status")
)
