from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "outer")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .filter(F.upper(F.col("p.product_name")).like("%OGA%"))
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(F.countDistinct(F.col("oi.unit_price")).alias("unique_orders"))
    .filter(F.col("unique_orders") > 1)
    .orderBy("launch_quarter")
)
