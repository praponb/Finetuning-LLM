from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("product_name").like("%Mat%"))
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "inner")
    .filter(F.col("S.total_stock_qty") > 1800)
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("S.last_launch_date"),
        F.col("S.total_stock_qty"),
    )
    .orderBy("total_stock_qty", F.col("rating").desc())
)
