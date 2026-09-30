from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .groupBy("supplier_id")
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.max_stock_qty") > 400)
    .select(F.col("s.rating"), F.col("ss.avg_stock_qty"), F.col("ss.max_stock_qty"))
    .orderBy(F.col("avg_stock_qty").desc())
)
