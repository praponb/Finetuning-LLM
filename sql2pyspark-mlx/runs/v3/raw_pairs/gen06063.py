from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4, 5]))
    .select(F.col("last_restock").alias("event_date"))
    .unionByName(
        df_stores
        .filter(F.col("store_name").isNull())
        .select(F.col("opened_date").alias("event_date"))
    )
)
