from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("order_date").alias("event_date"))
    .subtract(
        df_shipments
        .filter(F.col("cost").between(40, 44))
        .select(F.col("shipped_date").alias("event_date"))
    )
    .orderBy("event_date")
)
