from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").isNotNull())
    .select(F.col("delivered_date").alias("event_date"))
    .unionByName(
        df_inventory
        .filter(F.col("last_restock").isNotNull())
        .select(F.col("last_restock").alias("event_date"))
    )
    .distinct()
    .orderBy("event_date")
)
