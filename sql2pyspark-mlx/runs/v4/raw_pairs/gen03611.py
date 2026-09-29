from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        F.col("end_date").alias("event_date"),
        F.lit("subscription").alias("source"),
    )
    .unionByName(
        df_products
        .select(F.col("launch_date").alias("event_date"), F.lit("product").alias("source"))
    )
)
