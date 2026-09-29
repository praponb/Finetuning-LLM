from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 120)
    .select("end_date", F.lit("subscription").alias("source"))
    .union(
        df_shipments
        .filter(F.col("delivered_date") > "2024-05-21")
        .select("shipped_date", F.lit("shipment").alias("source"))
    )
    .distinct()
    .orderBy("end_date")
)
