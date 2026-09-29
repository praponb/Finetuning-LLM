from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.customer_id").isNotNull())
    .select(
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("s.city"),
        F.col("s.region"),
    )
    .orderBy(F.col("order_date").desc())
)
