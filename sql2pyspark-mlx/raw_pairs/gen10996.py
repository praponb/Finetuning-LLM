from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.min_stock_qty") >= 210)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("ss.first_launch_date"),
        F.col("ss.min_stock_qty"),
    )
)
