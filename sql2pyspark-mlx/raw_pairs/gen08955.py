from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("s.city"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(10)
)
