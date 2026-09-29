from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").isNotNull())
    .select("shipped_date")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date").isNotNull())
        .select(F.col("start_date").alias("shipped_date"))
    )
)
