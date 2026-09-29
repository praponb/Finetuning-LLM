from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2025-10-01")
    .select("status", F.lit("shipment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date").between("2024-10-01", "2024-12-01"))
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
)
