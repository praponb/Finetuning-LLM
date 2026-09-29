from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.max("stock_qty").alias("max_stock_qty"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .select(
        F.col("s.city"),
        F.col("s.rating"),
        F.col("ss.brand_list"),
        F.col("ss.max_stock_qty"),
    )
    .orderBy(F.col("city").desc())
)
