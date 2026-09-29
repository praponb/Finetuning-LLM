from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.count("*").alias("product_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.avg_stock_qty") > 220)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("ss.avg_stock_qty"),
        F.col("ss.product_count"),
    )
    .orderBy(F.col("avg_stock_qty").desc(), F.col("product_count").desc(), "id")
    .limit(10)
)
