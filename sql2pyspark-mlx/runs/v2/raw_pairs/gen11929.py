from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count("stock_qty").alias("stock_qty_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.avg_cost") > 630)
    .select(F.col("s.supplier_name"), F.col("ss.avg_cost"), F.col("ss.stock_qty_count"))
    .orderBy(F.col("stock_qty_count").desc())
)
