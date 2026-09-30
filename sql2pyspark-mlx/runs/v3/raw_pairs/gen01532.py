from pyspark.sql import functions as F

result = (
    df_products
    .select(F.col("launch_date").alias("event_date"))
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") != "enterprise")
        .select(F.col("start_date").alias("event_date"))
    )
    .distinct()
)
