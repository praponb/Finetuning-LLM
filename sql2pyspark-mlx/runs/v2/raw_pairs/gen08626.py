from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.lower(F.col("s.store_name")).like("%ld %"))
    .groupBy(F.col("s.region"))
    .agg(
        F.max(F.col("o.order_date")).alias("latest_order_date"),
        F.max(F.col("o.order_date")).alias("last_order_date"),
    )
    .orderBy("latest_order_date", "region")
    .limit(5)
)
