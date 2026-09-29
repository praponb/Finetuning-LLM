from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost") >= 620)
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "left")
    .select(F.col("s.country"), F.col("S.brand_values"), F.col("S.category_list"))
    .orderBy("country", "brand_values")
)
