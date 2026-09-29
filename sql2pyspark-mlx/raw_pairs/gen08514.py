from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(F.col("delivered_date").alias("event_date"))
    .intersect(
        df_accounts
        .filter(F.month("opened_date").isin([6, 10, 12]))
        .select(F.col("opened_date").alias("event_date"))
    )
    .orderBy("event_date")
)
