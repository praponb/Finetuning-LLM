from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .select(
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("o.status"),
        (F.col("o.discount") * F.col("o.shipping_fee")).alias("discount_times_shipping_fee"),
    )
)
