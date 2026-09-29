from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .intersect(
        df_shipments
        .filter(F.col("shipped_date") >= "2024-03-01")
        .select(F.col("delivered_date").alias("start_date"))
    )
    .orderBy("start_date")
)
