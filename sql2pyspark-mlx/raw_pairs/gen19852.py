from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("s.region"),
    )
    .orderBy("region")
)
