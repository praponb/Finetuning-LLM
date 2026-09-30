from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("customer_id").desc())
)
