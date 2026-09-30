from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.shipping_fee").between(18, 24))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("s.opened_date"),
        F.floor(F.col("o.amount")).alias("amount_whole"),
    )
)
