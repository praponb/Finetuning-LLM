from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 100)
    .select("end_date", F.lit("subscription").alias("source"))
    .union(
        df_inventory
        .filter(F.col("quantity") <= 84)
        .select("last_restock", F.lit("inventory").alias("source"))
    )
    .orderBy("source")
)
