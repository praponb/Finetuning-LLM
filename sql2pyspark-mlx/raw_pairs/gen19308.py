from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.shipping_fee") <= 29) & (F.col("s.shipped_date") < "2024-01-18"))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("o.discount"),
        (F.col("o.discount") - F.col("o.shipping_fee")).alias("discount_minus_shipping_fee"),
    )
)
