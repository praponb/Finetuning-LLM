from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount") > 0.3)
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.cost"),
        F.col("o.channel"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("cost").desc(), "id")
    .limit(5)
)
