from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .select(
        F.col("s.opened_date"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("customer_id").desc())
)
