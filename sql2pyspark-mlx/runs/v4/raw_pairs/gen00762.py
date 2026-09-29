from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.status") == "RETURNED")
    .groupBy(F.year(F.col("o.order_date")).alias("order_year"))
    .agg(
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
        F.avg(F.col("oi.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "order_year")
    .limit(5)
)
