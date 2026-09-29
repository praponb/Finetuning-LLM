from pyspark.sql import functions as F

orders_by_store = (
    df_orders
    .filter(F.col("shipping_fee") < 35)
    .groupBy("store_id")
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(orders_by_store.alias("obs"), F.col("s.id") == F.col("obs.store_id"), "left")
    .filter(F.col("obs.total_shipping_fee") > 140)
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("obs.total_shipping_fee"),
        F.col("obs.partner_count"),
    )
    .orderBy("id")
)
