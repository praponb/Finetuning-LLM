from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.region").isNotNull())
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("o.channel"),
    )
    .orderBy("channel")
)
