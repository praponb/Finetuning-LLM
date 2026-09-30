from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category") == "Grocery")
    .groupBy("supplier_id")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.min_stock_qty") >= 280)
    .select(
        F.col("s.country"),
        F.col("ss.min_stock_qty"),
        F.col("ss.first_launch_date"),
    )
)
