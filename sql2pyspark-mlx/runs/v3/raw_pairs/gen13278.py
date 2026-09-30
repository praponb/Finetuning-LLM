from pyspark.sql import functions as F

excluded_stores = df_stores.filter(F.lower(F.trim("store_name")) == "siam flagship").select("id")

result = (
    df_orders.alias("o")
    .join(excluded_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left_anti")
    .filter(F.col("o.store_id").isNotNull() | F.lit(excluded_stores.isEmpty()))
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("order_date").desc())
)
