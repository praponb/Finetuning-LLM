from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("price") > 970)
    .groupBy("supplier_id")
    .agg(
        F.max("stock_qty").alias("max_stock_qty"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .filter(F.col("ss.max_stock_qty") >= 310)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("ss.max_stock_qty"),
        F.col("ss.brand_list"),
    )
    .orderBy(F.col("max_stock_qty").desc(), "id")
    .limit(50)
)
