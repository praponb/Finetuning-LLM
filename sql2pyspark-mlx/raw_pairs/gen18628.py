from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2025-01-01")
    .select("last_restock", F.lit("inventory").alias("source"))
    .union(
        df_subscriptions
        .select("start_date", F.lit("subscription").alias("source"))
    )
    .distinct()
)
