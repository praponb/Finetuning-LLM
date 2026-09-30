from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount", F.lit("payment").alias("source"))
    .unionByName(
        df_shipments
        .select(F.col("cost").alias("amount"), F.lit("shipment").alias("source"))
    )
    .distinct()
    .orderBy("amount")
)
