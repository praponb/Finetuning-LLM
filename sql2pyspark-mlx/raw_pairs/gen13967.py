from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee", F.lit("subscription").alias("record_type"))
    .union(
        df_shipments
        .filter(F.col("status").isin(["LOST", "DELIVERED"]))
        .select("cost", F.lit("shipment").alias("record_type"))
    )
    .orderBy("record_type")
)
