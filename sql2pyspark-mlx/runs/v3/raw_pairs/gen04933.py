from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date").between("2023-03-31", "2025-03-01") | (F.col("s.sqft") < 6724))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.initcap(F.trim(F.col("s.region"))).alias("region_title"),
    )
)
