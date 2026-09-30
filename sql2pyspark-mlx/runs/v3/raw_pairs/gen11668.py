from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft") < 12175)
    .select(
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.date_trunc("year", F.col("o.order_date")).alias("order_date_period"),
    )
    .orderBy(F.col("store_id").desc())
)
