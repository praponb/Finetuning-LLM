from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.sqft") != 10233) & (F.col("s.city") == "Chiang Mai"))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
