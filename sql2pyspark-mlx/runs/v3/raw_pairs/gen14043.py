from pyspark.sql import functions as F

result = (
    df_inventory
    .select("last_restock", F.lit("inventory").alias("source"))
    .unionByName(
        df_subscriptions
        .select(
        F.col("start_date").alias("last_restock"),
        F.lit("subscription").alias("source"),
    )
    )
)
