from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .unionByName(
        df_subscriptions
        .filter(F.month("start_date").isin([2, 6, 12]))
        .select(F.col("start_date").alias("opened_date"))
    )
    .distinct()
)
