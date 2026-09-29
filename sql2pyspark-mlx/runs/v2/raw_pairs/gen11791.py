from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("status", F.lit("payment").alias("source"))
    .unionByName(
        df_shipments
        .filter(F.year("delivered_date") >= 2024)
        .select("status", F.lit("shipment").alias("source"))
    )
    .orderBy("source")
)
