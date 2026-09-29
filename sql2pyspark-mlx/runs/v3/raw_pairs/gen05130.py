from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.year(F.col("s.delivered_date")) >= 2025) & F.col("s.carrier").isNull())
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("cost").desc())
)
