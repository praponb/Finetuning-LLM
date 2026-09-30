from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Grocery", 1).otherwise(0))) / F.count("*")), 1).alias("pct_grocery"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.pct_grocery") > 76)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("ss.pct_grocery"),
        F.col("ss.total_stock_qty"),
    )
)
