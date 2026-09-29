from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .groupBy(F.col("s.city"), F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(
        F.max(F.col("o.shipping_fee")).alias("max_shipping_fee"),
        F.countDistinct(F.col("o.discount")).alias("unique_statuss"),
    )
)
