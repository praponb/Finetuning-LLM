from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "IN_TRANSIT", "LOST"]))
    .select(F.lit("shipment").alias("kind"), "delivered_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date").between("2023-10-01", "2024-01-01"))
        .select(
        F.lit("subscription").alias("kind"),
        F.col("end_date").alias("delivered_date"),
    )
    )
    .orderBy(F.col("kind").desc())
)
