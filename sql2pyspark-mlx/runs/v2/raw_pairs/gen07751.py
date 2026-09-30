from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.quantity") == 8)
    .groupBy(
        F.year(F.col("o.order_date")).alias("order_year"),
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
    )
    .agg(
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .orderBy("order_year", F.col("unit_price_range").desc())
)
