from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date", F.lit("store").alias("kind"))
    .unionByName(
        df_subscriptions
        .select(
        F.col("start_date").alias("opened_date"),
        F.lit("subscription").alias("kind"),
    )
    )
    .distinct()
)
