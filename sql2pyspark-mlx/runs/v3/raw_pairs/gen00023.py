from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 8, 25, 28]))
    .select("unit_price", F.lit("order_item").alias("source"))
    .unionByName(
        df_shipments
        .select(F.col("cost").alias("unit_price"), F.lit("shipment").alias("source"))
    )
    .distinct()
    .orderBy(F.col("source").desc())
)
