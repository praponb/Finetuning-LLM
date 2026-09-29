from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.store_id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("s.region"),
    )
)
