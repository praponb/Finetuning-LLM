from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost", F.lit("shipment").alias("source"))
    .unionByName(
        df_orders
        .select(F.col("amount").alias("cost"), F.lit("order").alias("source"))
    )
    .distinct()
)
