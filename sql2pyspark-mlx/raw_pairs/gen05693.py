from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").isNotNull())
    .select(F.col("last_restock").alias("event_date"))
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date").isNotNull())
        .select(F.col("start_date").alias("event_date"))
    )
)
