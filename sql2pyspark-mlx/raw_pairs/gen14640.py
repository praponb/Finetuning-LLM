from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.max_stock_qty") > 230)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("ss.min_stock_qty"),
        F.col("ss.max_stock_qty"),
    )
    .orderBy("max_stock_qty", "id")
    .limit(20)
)
