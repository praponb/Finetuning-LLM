from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("store_name").isNotNull()).alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("o.channel").isin(["mobile", "web", "partner"]))
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("o.customer_id"),
    )
)
