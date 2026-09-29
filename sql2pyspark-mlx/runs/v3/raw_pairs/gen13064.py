from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("stock_qty").alias("unique_suppliers"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("ss.unique_suppliers"),
        F.col("ss.brand_list"),
    )
    .orderBy(F.col("unique_suppliers").desc(), "id")
    .limit(3)
)
