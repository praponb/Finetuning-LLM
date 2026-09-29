from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("cost").alias("value"), F.lit("shipment").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["bank_transfer", "cash"]))
        .select(F.col("amount").alias("value"), F.lit("payment").alias("source"))
    )
    .orderBy("value")
)
