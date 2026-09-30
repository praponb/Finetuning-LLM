from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("sqft").desc())
)
