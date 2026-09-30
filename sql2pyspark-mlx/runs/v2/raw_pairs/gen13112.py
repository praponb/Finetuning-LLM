from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("unit_price").isNull())
    .groupBy("order_id")
    .agg(
        F.count("*").alias("num_order_items"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("O.num_order_items"),
        F.col("O.unit_price_range"),
    )
    .orderBy("channel")
)
