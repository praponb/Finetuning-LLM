from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.cost") > 520)
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"), F.col("p.brand"))
    .agg(F.max(F.col("oi.unit_price")).alias("max_unit_price"))
    .filter(F.col("max_unit_price") >= 310)
    .orderBy("max_unit_price", "launch_month", "brand")
    .limit(1)
)
