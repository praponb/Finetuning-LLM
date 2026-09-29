from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.city").isin(["Bangkok", "Tokyo", "Berlin"]))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.amount"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
    .orderBy(F.col("id").desc(), "region")
)
