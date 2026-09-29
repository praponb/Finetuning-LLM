from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.col("s.city"),
        F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.max(F.col("o.shipping_fee")).alias("max_shipping_fee"),
        F.min(F.col("o.order_date")).alias("first_order_date"),
    )
)
