from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("o.customer_id"),
    )
)
