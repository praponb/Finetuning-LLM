from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .groupBy(
        F.month(F.col("p.launch_date")).alias("launch_month"),
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
    )
    .agg(F.count(F.col("oi.unit_price")).alias("unit_price_count"))
    .filter(F.col("unit_price_count") >= 2)
    .orderBy(F.col("unit_price_count").desc(), "launch_month", "launch_quarter")
    .limit(3)
)
