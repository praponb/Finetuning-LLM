from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("s.order_id").isNotNull())
    .select(
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("s.carrier"),
        F.col("s.status"),
    )
    .orderBy(F.col("order_date").desc())
)
