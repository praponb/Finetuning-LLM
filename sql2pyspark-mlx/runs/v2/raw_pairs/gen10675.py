from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("o.amount") > 440)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
    .orderBy("sqft", "id")
    .limit(10)
)
