from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([4, 5, 8]))
    .select("status", F.lit("order").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") <= "2024-11-01")
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
)
