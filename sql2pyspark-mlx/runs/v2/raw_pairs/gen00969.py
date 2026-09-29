from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("s.store_name"),
        (F.col("o.shipping_fee") + F.col("o.amount")).alias("amount_with_shipping"),
    )
    .orderBy("store_name", F.col("id").desc())
)
