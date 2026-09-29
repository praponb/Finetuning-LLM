from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(~((F.col("o.discount") > 0.25) & F.month(F.col("o.order_date")).isin([2, 5, 10])))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
    )
)
