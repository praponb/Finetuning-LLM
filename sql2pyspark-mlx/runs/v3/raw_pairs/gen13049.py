from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.avg("stock_qty").alias("avg_stock_qty"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("supplier_summary"), F.col("suppliers.id") == F.col("supplier_summary.supplier_id"), "inner")
    .filter(F.col("supplier_summary.avg_stock_qty") >= 140)
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.city"),
        F.col("supplier_summary.price_range"),
        F.col("supplier_summary.avg_stock_qty"),
    )
)
