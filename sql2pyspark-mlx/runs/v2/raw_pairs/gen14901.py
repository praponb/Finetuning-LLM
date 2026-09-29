from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.size("tags") > 2)
    .groupBy("supplier_id")
    .agg(
        (F.max("price") - F.min("price")).alias("price_range"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_stats.alias("supplier_stats"), F.col("suppliers.id") == F.col("supplier_stats.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.supplier_name"),
        F.col("supplier_stats.price_range"),
        F.col("supplier_stats.max_stock_qty"),
    )
    .orderBy(F.col("price_range").desc())
)
