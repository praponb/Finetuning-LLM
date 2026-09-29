from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.dayofmonth(F.col("s.opened_date")).alias("opened_date_day"),
    )
)
