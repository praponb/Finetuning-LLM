from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.store_id").isin([33, 38]))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("s.city"),
        F.col("s.opened_date"),
    )
)
