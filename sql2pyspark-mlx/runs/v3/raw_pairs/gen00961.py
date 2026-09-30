from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([19, 26, 30]))
    .select("cost", F.lit("shipment").alias("source"))
    .unionByName(
        df_orders
        .select(F.col("amount").alias("cost"), F.lit("order").alias("source"))
    )
)
