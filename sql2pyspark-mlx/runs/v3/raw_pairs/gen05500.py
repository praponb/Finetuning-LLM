from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost") > 440)
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("S"), F.col("s.id") == F.col("S.supplier_id"), "left")
    .filter(F.col("S.fashion_count") >= 6)
    .select(
        F.col("s.supplier_name"),
        F.col("S.category_list"),
        F.col("S.fashion_count"),
    )
    .orderBy("supplier_name")
)
