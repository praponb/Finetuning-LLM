from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(10, 13))
    .select("amount", F.lit("payment").alias("source"))
    .union(
        df_shipments
        .filter(F.year("delivered_date") >= 2024)
        .select("cost", F.lit("shipment").alias("source"))
    )
    .distinct()
    .orderBy(F.col("amount").desc())
)
