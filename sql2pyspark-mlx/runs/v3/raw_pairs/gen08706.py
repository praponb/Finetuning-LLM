from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("s.sqft"),
        F.col("o.store_id"),
        F.lower(F.col("s.store_name")).alias("store_name_lower"),
    )
)
