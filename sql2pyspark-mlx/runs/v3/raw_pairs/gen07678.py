from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.quantity") == 8)
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"), F.col("o.status"))
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .orderBy("order_year", F.col("max_quantity").desc())
)
