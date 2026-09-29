from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("supplier_id") == 16)
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(F.col("s.country"), F.col("ss.min_launch_date"))
    .orderBy("min_launch_date")
)
