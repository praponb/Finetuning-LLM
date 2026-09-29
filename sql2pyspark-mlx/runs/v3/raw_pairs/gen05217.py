from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"), F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(
        (F.max(F.col("o.shipping_fee")) - F.min(F.col("o.shipping_fee"))).alias("shipping_fee_range"),
    )
    .orderBy(F.col("shipping_fee_range").desc(), "region", "opened_month")
    .limit(50)
)
