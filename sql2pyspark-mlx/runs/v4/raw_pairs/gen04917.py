from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .unionByName(
        df_subscriptions
        .filter(F.col("status") != "EXPIRED")
        .select(F.col("start_date").alias("opened_date"))
    )
    .distinct()
)
