from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.sqft") < 14388)
    .select(
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("o.channel"),
    )
    .orderBy("store_name")
)
