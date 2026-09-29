from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("s.status").isin(["RETURNED", "DELIVERED", "IN_TRANSIT"]) & (F.col("o.order_date") > "2025-08-30"))
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("s.cost"),
        F.col("s.shipped_date"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("o.order_date"),
    )
)
