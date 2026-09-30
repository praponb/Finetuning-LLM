from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select(F.col("shipped_date").alias("event_date"))
    .unionByName(
        df_inventory
        .filter(F.col("quantity").isNull())
        .select(F.col("last_restock").alias("event_date"))
    )
)
