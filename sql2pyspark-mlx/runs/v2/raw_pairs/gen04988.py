from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.store_id").isNull())
    .select(
        F.col("s.sqft"),
        F.col("o.store_id"),
        F.col("o.channel"),
        F.length(F.col("s.store_name")).alias("store_name_length"),
    )
)
