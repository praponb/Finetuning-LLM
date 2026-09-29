from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost", F.lit("shipment").alias("source"))
    .unionByName(
        df_payments
        .select(F.col("amount").alias("cost"), F.lit("payment").alias("source"))
    )
    .orderBy(F.col("cost").desc())
)
