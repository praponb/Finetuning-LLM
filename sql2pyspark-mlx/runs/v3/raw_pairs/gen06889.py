from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.store_name").like("%Old%") & (F.col("s.sqft") < 10347))
    .select(
        F.col("s.region"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("region").desc())
)
