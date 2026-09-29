from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("stock_qty")).otherwise(0)).alias("umbrella_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(F.col("s.id"), F.col("s.rating"), F.col("t.umbrella_stock_qty"))
)
