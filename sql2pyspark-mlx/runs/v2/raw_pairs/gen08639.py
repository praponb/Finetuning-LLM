from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.category") == "Grocery")
    .groupBy(F.year(F.col("p.launch_date")).alias("launch_year"))
    .agg(F.max(F.col("oi.unit_price")).alias("max_unit_price"))
    .orderBy(F.col("launch_year").desc())
    .limit(20)
)
