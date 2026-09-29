from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("o.order_date")).isin([2, 5, 10]))
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.avg(F.col("o.shipping_fee")), 2).alias("avg_shipping_fee"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.status")))).alias("status_values"),
    )
)
