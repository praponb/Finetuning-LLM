from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.order_id").isNull() | (F.col("o.shipping_fee") < 16))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("o.order_date"),
    )
)
