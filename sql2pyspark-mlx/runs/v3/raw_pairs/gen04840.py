from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .filter(F.col("o.amount").between(450, 880) & (F.col("s.region") == "East"))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("s.city"),
        F.col("s.region"),
        F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_date_label"),
    )
    .orderBy("region")
)
