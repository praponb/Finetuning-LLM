from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("s.order_id").isNotNull())
    .select(
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("s.cost"),
    )
    .orderBy(F.col("customer_id").desc())
)
