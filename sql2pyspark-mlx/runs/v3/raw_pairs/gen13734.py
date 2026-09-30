from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "right")
    .groupBy(F.col("p.brand"), F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.max(F.col("oi.unit_price")).alias("max_unit_price"))
)
