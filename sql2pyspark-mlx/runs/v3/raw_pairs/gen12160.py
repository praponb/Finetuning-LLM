from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft") != 12908)
    .select(
        F.col("o.status"),
        F.col("o.amount"),
        F.col("o.channel"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
    )
)
