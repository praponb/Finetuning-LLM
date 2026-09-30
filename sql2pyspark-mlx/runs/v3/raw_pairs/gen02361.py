from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.opened_date").between("2023-10-16", "2024-07-01"))
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.min(F.col("o.shipping_fee")).alias("min_shipping_fee"),
        F.max(F.col("o.discount")).alias("max_discount"),
    )
)
