from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(66, 71))
    .select("status", F.lit("shipment").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") == "standard")
        .select("status", F.lit("subscription").alias("source"))
    )
    .orderBy(F.col("source").desc())
)
