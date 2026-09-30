from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("category") == "Sports")
    .groupBy("supplier_id")
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.stock_qty_range") >= 21)
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("t.stock_qty_range"),
        F.col("t.total_stock_qty"),
    )
)
