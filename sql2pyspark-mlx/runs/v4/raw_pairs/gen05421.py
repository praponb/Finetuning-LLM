from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("stock_qty").between(131, 334))
    .groupBy("supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.min("launch_date").alias("min_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("s.city"),
        F.col("ss.stock_qty_range"),
        F.col("ss.min_launch_date"),
    )
    .orderBy(F.col("id").desc())
)
