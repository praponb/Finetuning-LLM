from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.year(F.col("s.delivered_date")) == 2023) & (F.col("o.discount") > 0.22))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.carrier"),
        F.col("s.status"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.round((F.col("s.cost") * 100), 2).alias("cost_adjusted"),
    )
)
