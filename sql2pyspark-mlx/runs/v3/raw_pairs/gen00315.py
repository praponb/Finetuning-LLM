from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("cost").alias("value"), F.lit("shipment").alias("source"))
    .unionByName(
        df_order_items
        .select(F.col("unit_price").alias("value"), F.lit("order_item").alias("source"))
    )
    .orderBy("value")
)
