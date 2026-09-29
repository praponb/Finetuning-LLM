from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("oi.unit_price") < 150)
    .groupBy(F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"))
    .agg(F.sum(F.col("oi.unit_price")).alias("total_unit_price"))
)
