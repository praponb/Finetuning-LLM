from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter((F.col("s.sqft") != 7280) & (F.col("o.shipping_fee") > 37))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
    )
)
