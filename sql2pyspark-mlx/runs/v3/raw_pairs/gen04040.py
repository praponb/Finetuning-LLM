from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter((F.col("s.region") == "North") & (F.year(F.col("s.opened_date")) >= 2024))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
)
