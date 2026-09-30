from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        F.col("end_date").alias("event_date"),
        F.lit("subscription").alias("record_type"),
    )
    .unionByName(
        df_stores
        .select(
        F.col("opened_date").alias("event_date"),
        F.lit("store").alias("record_type"),
    )
    )
    .distinct()
    .orderBy("event_date")
)
