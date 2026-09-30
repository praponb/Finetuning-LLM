from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2025-07-01")
    .select(F.col("start_date").alias("event_date"))
    .unionByName(
        df_products
        .filter(F.col("category") == "Home")
        .select(F.col("launch_date").alias("event_date"))
    )
    .distinct()
)
