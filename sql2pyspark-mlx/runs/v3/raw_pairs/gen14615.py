from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("id").desc(), F.col("order_date").desc())
)
