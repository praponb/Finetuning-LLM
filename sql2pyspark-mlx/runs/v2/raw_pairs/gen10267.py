from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("product_name").like("Lap%"))
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .select(F.col("s.id"), F.col("s.rating"), F.col("t.min_launch_date"))
)
