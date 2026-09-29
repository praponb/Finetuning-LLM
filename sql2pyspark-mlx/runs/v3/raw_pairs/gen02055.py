from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("supplier_id") == 23)
    .groupBy("supplier_id")
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "left")
    .filter(F.col("t.avg_stock_qty") > 33)
    .select(F.col("s.rating"), F.col("t.max_stock_qty"), F.col("t.avg_stock_qty"))
)
