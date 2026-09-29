from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(~(F.col("o.status").isin(["PENDING", "COMPLETED"]) | F.col("s.region").isin(["North", "Central"])))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("o.store_id"),
        F.col("o.status"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.coalesce(F.col("oi.quantity"), F.lit(0)).alias("quantity_filled"),
    )
    .orderBy(F.col("id").desc())
)
