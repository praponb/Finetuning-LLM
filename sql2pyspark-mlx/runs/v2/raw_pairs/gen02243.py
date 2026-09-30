from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.month(F.col("p.launch_date")).isin([6, 9, 11]))
    .groupBy(F.col("s.country"))
    .agg(F.round(F.stddev(F.col("oi.unit_price")), 2).alias("stddev_unit_price"))
    .orderBy("stddev_unit_price", "country")
    .limit(50)
)
