from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("source"))
    .union(
        df_inventory
        .filter(F.col("quantity") >= 128)
        .select("last_restock", F.lit("inventory").alias("source"))
    )
    .orderBy("source")
)
