from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .groupBy("supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.countDistinct("stock_qty").alias("unique_suppliers"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.rating"),
        F.col("t.first_launch_date"),
        F.col("t.unique_suppliers"),
    )
)
