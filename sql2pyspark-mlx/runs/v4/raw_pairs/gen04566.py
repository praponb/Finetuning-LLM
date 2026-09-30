from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("o.discount").between(0.11, 0.3))
    .select(
        F.col("s.region"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.add_months(F.col("o.order_date"), 3).alias("order_date_plus_months"),
    )
    .orderBy("store_id")
)
