from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("amount").alias("value"), F.lit("order").alias("source"))
    .unionByName(
        df_shipments
        .select(F.col("cost").alias("value"), F.lit("shipment").alias("source"))
    )
)
