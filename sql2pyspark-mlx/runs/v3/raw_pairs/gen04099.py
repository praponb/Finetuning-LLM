from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("cost") > 360)
    .groupBy("supplier_id")
    .agg(F.min("stock_qty").alias("min_stock_qty"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.min_stock_qty") > 380)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.rating"),
        F.col("ss.min_stock_qty"),
    )
)
