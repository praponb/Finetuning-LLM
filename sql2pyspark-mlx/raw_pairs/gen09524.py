from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").isNotNull())
    .select(F.col("order_date").alias("event_date"))
    .unionByName(
        df_accounts
        .filter(F.col("opened_date").isNotNull())
        .select(F.col("opened_date").alias("event_date"))
    )
    .distinct()
    .orderBy("event_date")
)
