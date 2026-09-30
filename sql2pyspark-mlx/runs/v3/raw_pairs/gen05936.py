from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock", F.lit("inventory").alias("source"))
    .unionByName(
        df_shipments
        .select(
        F.col("delivered_date").alias("last_restock"),
        F.lit("shipment").alias("source"),
    )
    )
)
