from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.initcap(F.trim(F.col("s.region"))).alias("region_title"),
    )
    .orderBy("opened_date", "id")
    .limit(5)
)
