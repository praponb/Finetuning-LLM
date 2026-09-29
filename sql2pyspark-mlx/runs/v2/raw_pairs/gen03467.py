from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status", F.lit("shipment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.year("end_date") == 2025)
        .select("status", F.lit("subscription").alias("source"))
    )
    .orderBy("status")
)
