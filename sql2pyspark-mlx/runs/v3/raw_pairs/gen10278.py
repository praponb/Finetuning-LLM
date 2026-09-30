from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("s.region"),
    )
    .orderBy(F.col("status").desc())
)
