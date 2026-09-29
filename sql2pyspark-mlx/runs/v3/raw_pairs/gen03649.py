from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("o.customer_id") == 10) & F.col("s.opened_date").between("2023-06-01", "2025-08-11"))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.initcap(F.trim(F.col("s.store_name"))).alias("store_name_title"),
    )
    .orderBy("store_name")
)
